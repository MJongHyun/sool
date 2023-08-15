package sool.extract.run

import sool.address.run.RunBldngMgmtNmbAddr.slack
import sool.common.check_args.{CheckArgsRun, CheckDt, CheckIp}
import sool.common.function.Slack
import sool.extract.extract_data.{ExtrctBase, ExtrctDtiCh3, ExtrctDtiRetail, ExtrctRetail, ExtrctVendor, ExtractCumul}
import sool.service.run.RunService.cmnFuncCls

object RunExtract {
  // 필요 클래스 선언
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()
  val checkArgsRunCls = new CheckArgsRun()
  val checkIpCls = new CheckIp()
  val checkDtCls = new CheckDt()
  val extrctVendorRawCls = new ExtrctVendor(spark)
  val extrctDtiRetailCls = new ExtrctDtiRetail(spark)
  val extrctDtiCh3Cls = new ExtrctDtiCh3(spark)
  val extrctRetailCls = new ExtrctRetail(spark)
  val extrctBaseCls = new ExtrctBase(spark)
  val extrctCumuls = new ExtractCumul(spark)
  val slack = new Slack()

  def main(args: Array[String]): Unit = {
    val (ethDt, flag) = (args(0), args(1))
    val fileName = this.getClass.getSimpleName.stripSuffix("$")

    logger.info(s"[file=RunExtract] [function=main] [status=start] [message=${ethDt} ${flag} 데이터 추출 작업을 시작합니다.]")

    try {
      val (checkIntCntInEthDtBool, checkYMFrmtBool) = checkArgsRunCls.checkServiceArgsVals(ethDt)
      val checkIp = checkIpCls.checkIp()
      if (checkIntCntInEthDtBool == false || checkYMFrmtBool == false || checkIp == false) {
        logger.error(s"[file=RunExtract] [function=main] [status=error] [message=집계연월 및 IP 를 확인하세요.]")
        return
      }
      checkDtCls.checkAggYM(ethDt) // 현재 연월, 예상 집계 연월, 실행 집계 연월 info 제공
      /* 도매상 리스트 업데이트(vendor.parquet) */
      slack.sendMsg(flag, "유흥거래 데이터 추출 시작", fileName)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=도매상 리스트 업데이트 시작]")
      extrctVendorRawCls.extrctVendor(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=도매상 리스트 업데이트 완료]")

      /* 소매 데이터 추출(dtiRetail.parquet) */
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=소매 데이터 추출 시작]")
      extrctDtiRetailCls.extrctDtiRetail(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=소매 데이터 추출 완료]")

      /* 유흥 데이터 추출(dtiChannel3.parquet) */
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=유흥 데이터 추출 시작]")
      extrctDtiCh3Cls.extrctDtiCh3(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=유흥 데이터 추출 완료]")

      /* 소매상 리스트 업데이트(newCom.parquet, retail.parquet) */
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=소매상 리스트 업데이트 시작]")
      extrctRetailCls.extrctRetail(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=소매상 리스트 업데이트 완료]")

      /* 분석 기초 데이터 생성(base.parquet) */
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=분석 기초 데이터 생성 시작]")
      extrctBaseCls.extrctBase(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=분석 기초 데이터 생성 완료]")

      /* 인보이스일 경우 2달전 dti 누적사전 inv경로에 저장 */
      if (flag=="inv"){
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=INV 누적사전 저장 시작]")
        extrctCumuls.extrctCumul(ethDt, flag)
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=INV 누적사전 저장 완료]")
      }
      slack.sendMsg(flag, "유흥거래 데이터 추출 완료", fileName)
    } catch {
      case ex: Exception => logger.error(s"[file=RunExtract] [function=main] [status=error] [message=${ex}]")
        slack.sendErrorMsg(flag, "유흥거래 데이터 추출", fileName, ex.toString)
    } finally {
      logger.info(s"[file=RunExtract] [function=main] [status=end] [message=${ethDt} ${flag} 데이터 추출 작업을 종료합니다.]")
    }
  }
}
