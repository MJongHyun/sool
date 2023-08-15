package sool.verification.run

import sool.common.check_args.{CheckArgsRun, CheckDt, CheckIp}
import sool.common.function.Slack
import sool.service.run.RunService.cmnFuncCls
import sool.verification.verfiy.VerifyHK

object RunVerifyHk {
  // 필요 클래스 선언
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()
  val checkArgsRunCls = new CheckArgsRun()
  val checkIpCls = new CheckIp()
  val checkDtCls = new CheckDt()
  val verifyData = new VerifyHK(spark)
  val slack = new Slack()

  def main(args: Array[String]): Unit = {
    // ethDt, opt1 : (com, newcom)
    val (ethDt, flag) = (args(0), args(1))
    val fileName = this.getClass.getSimpleName.stripSuffix("$")

    logger.info(s"[file=RunExtract] [function=main] [status=start] [message=${ethDt} ${flag} HK 검증 시작합니다.]")
    try {
      val (checkIntCntInEthDtBool, checkYMFrmtBool) = checkArgsRunCls.checkServiceArgsVals(ethDt)
      val checkIp = checkIpCls.checkIp()
      if (checkIntCntInEthDtBool == false || checkYMFrmtBool == false || checkIp == false) {
        logger.error(s"[file=RunExtract] [function=main] [status=error] [message=집계연월 및 IP 를 확인하세요.]")
        return
      }
      checkDtCls.checkAggYM(ethDt) // 현재 연월, 예상 집계 연월, 실행 집계 연월 info 제공
      /* HK검증값 */
      slack.sendMsg(flag, "HK 검증 시작", fileName)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=HK 분모값 0 혹은 음수값 확인 시작]")
      verifyData.runVerifyHK(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=HK 분모값 0 혹은 음수값 확인 완료]")
      slack.sendMsg(flag, "HK 검증 완료", fileName)
    } catch {
      case ex: Exception => logger.error(s"[file=RunExtract] [function=main] [status=error] [message=${ex}]")
        slack.sendErrorMsg(flag, "HK 검증 완료", fileName, ex.toString)
    } finally {
      logger.info(s"[file=RunExtract] [function=main] [status=end] [message=${ethDt} ${flag}  HK 검증 종료합니다.]")
    }
  }
}
