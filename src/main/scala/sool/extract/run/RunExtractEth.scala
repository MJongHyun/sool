package sool.extract.run

import sool.common.check_args.{CheckArgsRun, CheckDt, CheckIp}
import sool.common.function.Slack
import sool.extract.extract_data._
import sool.service.run.RunService.cmnFuncCls

object RunExtractEth {
  // 필요 클래스 선언
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()
  val checkArgsRunCls = new CheckArgsRun()
  val checkIpCls = new CheckIp()
  val checkDtCls = new CheckDt()

  val NonEthUpdate = new NonEthComFilter(spark)
  val extrctEths = new ExtractEthanol(spark)
  val checkDataClts = new CheckDataCollection(spark)
  val slack = new Slack()

  def main(args: Array[String]): Unit = {
    val (ethDt, flag) = (args(0), args(1))
    val fileName = this.getClass.getSimpleName.stripSuffix("$")

    logger.info(s"[file=RunExtract] [function=main] [status=start] [message=${ethDt} ${flag} 주류 데이터 추출을 시작합니다.]")

    try {
      val (checkIntCntInEthDtBool, checkYMFrmtBool) = checkArgsRunCls.checkServiceArgsVals(ethDt)
      val checkIp = checkIpCls.checkIp()
      if (checkIntCntInEthDtBool == false || checkYMFrmtBool == false || checkIp == false) {
        logger.error(s"[file=RunExtract] [function=main] [status=error] [message=집계연월 및 IP 를 확인하세요.]")
        return
      }
      checkDtCls.checkAggYM(ethDt) // 현재 연월, 예상 집계 연월, 실행 집계 연월 info 제공
      slack.sendMsg(flag, "주류 도매상 업체 & 거래 추출 시작", fileName)
      if (flag=="dti"){
        //주류 데이터 추출 시작 1-0, 1-1
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=주류 데이터 추출 시작]")
        extrctEths.extractEthanolData(ethDt, flag)
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=주류 데이터 추출 완료]")

        //데이터 정합성 검사 실행 1-2
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=데이터 정합성 검사 시작]")
        checkDataClts.dataCollet(ethDt, flag)
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=데이터 정합성 검사 완료]")
        slack.sendMsg(flag, "주류 도매상 업체 & 거래 추출 완료", fileName)
      }
      else if (flag=="inv"){
        //주류 데이터 추출 시작 1-0, 1-1
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=주류 데이터 추출 시작]")
        extrctEths.extractEthanolDataInv(ethDt, flag)
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=주류 데이터 추출 완료]")
        slack.sendMsg(flag, "주류 도매상 업체 & 거래 추출 완료", fileName)
      }
    } catch {
      case ex: Exception => logger.error(s"[file=RunExtract] [function=main] [status=error] [message=${ex}]")
        slack.sendErrorMsg(flag, "주류 도매상 업체 & 거래 추출", fileName, ex.toString)
    } finally {
      logger.info(s"[file=RunExtract] [function=main] [status=end] [message=${ethDt} ${flag} 주류 데이터 추출을 종료합니다.]")
    }
  }
}
