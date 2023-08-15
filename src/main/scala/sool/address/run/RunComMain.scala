/**
 * 모집단 데이터 추출, 사업자별 대표 주소 선정 및 결과 저장, 업체 마스터 생성 및 저장

 */

package sool.address.run

import sool.address.refine_addr.{AddrMatchingDivOrdr, BldngMgmtNmbAddr, ComMain, ComMainCsv, ComRgnoRprsnBmnCn, MoAddr}
import sool.common.function.{CmnFunc, FileFunc, GetTime, Slack}
import sool.common.jdbc.{JdbcGet, JdbcSave}
import sool.common.path.FilePath
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions.lit
import sool.address.run.RunBldngMgmtNmbAddr2.slack

object RunComMain {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val rfnAddrCls = new BldngMgmtNmbAddr(spark)
  val jdbcGetCls = new JdbcGet(spark)
  val jdbcSaveCls = new JdbcSave()
  val getTimeCls = new GetTime()
  val addrMatchingDivOrdrCls = new AddrMatchingDivOrdr(spark)
  val comRgnoRprsnBmnCnCls = new ComRgnoRprsnBmnCn(spark)
  val comMainCsvCls = new ComMainCsv(spark)
  val comMainCls = new ComMain(spark)
  val moAddrCls = new MoAddr(spark)
  val slack = new Slack()

  def main(args: Array[String]):Unit = {
    import spark.implicits._

    val (ethDt, flag) = (args(0), args(1))
    val filePathCls = new FilePath(ethDt, flag)
    val ethDtBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethDtBf1m, flag)
    val fileName = this.getClass.getSimpleName.stripSuffix("$")

    slack.sendMsg(flag, "업체마스터 생성 시작", fileName)
    logger.info(s"[file=RunComMain] [function=main] [status=end] [message=${ethDt} 업체 마스터 생성 작업 시작]")

    // 필요 데이터
    val dtiAddr = jdbcGetCls.getAddressTbl("DTI_ADDRESS")
    val base = fileFuncCls.rParquet(filePathCls.basePath)
    // [2022.11.07] fix addr 파일에서 DB로 변경
    val fixAddrLoad = jdbcGetCls.getAddressTbl("FIX_ADDR").filter($"USE_YN"==="Y").drop("USE_YN","DESC_TXT","REG_DT","UPDT_DT")
    val comMainBf1m = fileFuncCls.rParquet(filePathClsBf1m.comMainPath)
    val comMainPre = fileFuncCls.rParquet(filePathCls.comMainPrePath)

    // 모집단 데이터 추출
    logger.info(s"[file=RunComMain] [function=main] [status=running] [message=모집단 데이터 추출 및 저장 시작]")
    val moAddr = moAddrCls.runMoAddr(ethDtBf1m, base, flag)
    fileFuncCls.wCsv(moAddr, filePathCls.moAddrPath, "true")  // moAddr_yyyymm.csv
    logger.info(s"[file=RunComMain] [function=main] [status=running] [message=모집단 데이터 추출 및 저장 완료]")

    // 모집단 데이터 로드 및 인덱싱
    val moAddrIdx = moAddr.withColumn("IDX", monotonically_increasing_id())
    // 모집단 데이터에 건물관리번호 매칭 및 모집단별 순서 컬럼 추가
    val addrMatchingDivOrdr = addrMatchingDivOrdrCls.runAddrMatchingDivOrdr(moAddrIdx, dtiAddr)
    // 사업자별 대표 건물관리번호, 사업자명, LEN 데이터프레임 생성
    val comRgnoRprsnBmnCnLen = comRgnoRprsnBmnCnCls.runComRgnoRprsnBmnCnLen(addrMatchingDivOrdr)

    /* 건물정보 DB VIEW 로드 */
    // clickhouse DB로 변경 (파티셔닝 처리 안해도 안터짐)
    logger.info(s"[file=RunComMain] [function=main] [status=running] [message=건물정보 DB VIEW cache 시작]")
    val bldngInfoView = jdbcGetCls.getBldngInfoView().cache()
    bldngInfoView.count
    logger.info(s"[file=RunComMain] [function=main] [status=running] [message=건물정보 DB VIEW cache 완료]")

    /* 사업자별 대표 주소 선정 및 결과 저장 */
    val comMainCsv = comMainCsvCls(comRgnoRprsnBmnCnLen, bldngInfoView)
    logger.info(s"[file=RunComMain] [function=main] [status=running] [message=사업자별 대표 주소 결과 저장 시작]")
    fileFuncCls.wCsv(comMainCsv, filePathCls.comMainCsvPath, "false")   // com_main_csv.parquet
    logger.info(s"[file=RunComMain] [function=main] [status=running] [message=사업자별 대표 주소 결과 저장 완료]")
    bldngInfoView.unpersist()

    /* com_main.parquet 생성 */
    val comMainCsvLoad = fileFuncCls.rCsv(filePathCls.comMainCsvPath, "false")
    val (comMain, comMainDk) = comMainCls.getComMain(ethDt, base, comMainCsvLoad, fixAddrLoad, comMainBf1m, comMainPre)
    logger.info(s"[file=RunComMain] [function=main] [status=running] [message=업체 마스터 저장 시작]")
    fileFuncCls.wParquet(comMain, filePathCls.comMainPath)  // com_main.parquet
    fileFuncCls.wParquet(comMainDk, filePathCls.comMainDkPath)  // com_main_dk.parquet
    logger.info(s"[file=RunComMain] [function=main] [status=running] [message=업체 마스터 저장 완료]")
    val checkComMain = fileFuncCls.rParquet(filePathCls.comMainPath)  // 체크 목적으로 사용
    comMainCls.checkComMain(checkComMain, ethDt) // check
    logger.info(s"[file=RunComMain] [function=main] [status=end] [message=${ethDt} 업체 마스터 생성 작업 종료]")
    slack.sendMsg(flag, "업체마스터 생성 완료", fileName)
  }
}
