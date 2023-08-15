/**
 * 다음 API를 통해 각 원천 주소에 해당하는 건물관리번호를 구하여 DB 에 저장하는 코드

 */

package sool.address.run

import sool.address.refine_addr.{BldngMgmtNmbAddr, RawAddr}
import sool.common.function.{CmnFunc, FileFunc, Slack}
import sool.common.path.FilePath
import sool.common.jdbc.JdbcSave
import sool.service.run.RunService.logger

object RunBldngMgmtNmbAddr {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // 필요 클래스 선언
  val rawAddrCls = new RawAddr(spark)
  val fileFuncCls = new FileFunc(spark)
  val rfnAddrCls = new BldngMgmtNmbAddr(spark)
  val jdbcSaveCls = new JdbcSave()
  val slack = new Slack()

  // 총 작업시간: 202108 로 테스트 시 약 1시간 30분
  def main(args: Array[String]):Unit = {
    val (ethDt, flag) = (args(0), args(1))
    val filePathCls = new FilePath(ethDt, flag)
    val fileName = this.getClass.getSimpleName.stripSuffix("$")

    slack.sendMsg(flag, "원천주소 추출 및 원천주소별 건물관리번호 수집 시작", fileName)
    logger.info(s"[file=RunBldngMgmtNmbAddr] [function=main] [status=start] [message=${ethDt} ${flag} RunBldngMgmtNmbAddr 작업 시작]")
    /* 원천 주소 추출 및 저장 */
    logger.info(s"[file=RunBldngMgmtNmbAddr] [function=main] [status=running] [message=원천 주소 추출 및 저장 시작]")

    val dtiEthanol = fileFuncCls.rParquet(filePathCls.dtiEthPath)
    val rawAddr = rawAddrCls(dtiEthanol)
    fileFuncCls.wCsv(rawAddr, filePathCls.rawAddrPath, "true") // 저장
    logger.info(s"[file=RunBldngMgmtNmbAddr] [function=main] [status=running] [message=원천 주소 추출 및 저장 완료]")

    /*
    다음 API를 통해 각 원천 주소에 해당하는 건물관리번호를 구하는 작업(수집)
    백업용 및 추후 작업 효율을 높이기 위해 임시 저장
     */
    logger.info(s"[file=RunBldngMgmtNmbAddr] [function=main] [status=running] [message=원천 주소별 건물관리번호 수집 데이터 cache 시작]")
    val bldngMgmtNmbAddr = rfnAddrCls.getBldngMgmtNmbAddr(rawAddr).cache()
    bldngMgmtNmbAddr.count
    logger.info(s"[file=RunBldngMgmtNmbAddr] [function=main] [status=running] [message=원천 주소별 건물관리번호 수집 데이터 cache 완료]")

    /* 정제된 주소 DB 저장(21년 8월 데이터로 테스트 결과 2분 정도 소요됨) */
    logger.info(s"[file=RunBldngMgmtNmbAddr] [function=main] [status=running] [message=원천 주소별 건물관리번호 수집 데이터를 DB 에 저장 시작]")
    val bldngMgmtNmbAddrForDb = rfnAddrCls.getBldngMgmtNmbAddrForDb(bldngMgmtNmbAddr)
    jdbcSaveCls.upsertDtiAdrs(bldngMgmtNmbAddrForDb)  // DB Upsert
    logger.info(s"[file=RunBldngMgmtNmbAddr] [function=main] [status=running] [message=원천 주소별 건물관리번호 수집 데이터를 DB 에 저장 완료]")
    bldngMgmtNmbAddr.unpersist()
    logger.info(s"[file=RunBldngMgmtNmbAddr] [function=main] [status=end] [message=${ethDt} ${flag} RunBldngMgmtNmbAddr 작업 종료]")
    slack.sendMsg(flag, "원천주소 추출 및 원천주소별 건물관리번호 수집 완료", fileName)
  }
}