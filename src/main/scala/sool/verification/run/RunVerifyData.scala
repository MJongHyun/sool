package sool.verification.run

import sool.common.check_args.{CheckArgsRun, CheckDt, CheckIp}
import sool.common.function.Slack
import sool.extract.run.RunExtract.checkDtCls
import sool.verification.verfiy.{MakeMail, VerifyData, VerifyDataMart}
import sool.service.run.RunService.cmnFuncCls

object RunVerifyData {
  // spark, logger
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // 필요 클래스 선언
  val checkArgsRunCls = new CheckArgsRun()
  val checkDtCls = new CheckDt()
  val checkIpCls = new CheckIp()
  val checkData = new VerifyData(spark)
  val checkDataMart = new VerifyDataMart(spark)
  val mailData = new MakeMail(spark)
  val slack = new Slack()

  // 메인
  def main(args: Array[String]): Unit = {
    // args(0): 집계연월
    val (ethDt, flag) = (args(0), args(1))
    val fileName = this.getClass.getSimpleName.stripSuffix("$")

    logger.info(s"[file=RunService] [function=main] [status=start] [message=${ethDt} ${flag} data verify files 저장 시작합니다.]")
    try {
      val (checkIntCntInEthDtBool, checkYMFrmtBool) = checkArgsRunCls.checkServiceArgsVals(ethDt)
      val checkIp = checkIpCls.checkIp()
      if (checkIntCntInEthDtBool == false || checkYMFrmtBool == false || checkIp == false) {
        logger.error(s"[file=RunService] [function=main] [status=error] [message=집계연월 및 IP 를 확인하세요.]")
        return
      }
      checkDtCls.checkAggYM(ethDt) // 현재 연월, 예상 집계 연월, 실행 집계 연월 info 제공
      /* 시계열 추이 엑셀 생성 */
      slack.sendMsg(flag, "검증 데이터 추출 시작", fileName)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=data verify files 저장 시작]")
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=시계열 추이 엑셀 저장 시작]")
      checkData.runVerifyData(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=시계열 추이 엑셀 저장 완료]")
      /* 데이터 마트 검증 파일 생성 */
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=데이터마트 검증파일 저장 시작]")
      checkDataMart.runVerifyDataMart(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=데이터마트 검증파일 저장 완료]")
      /* 메일 작성 관련 파일 생성 */
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=메일 전송 관련 파일 저장 시작]")
      mailData.runMkMail(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=메일 전송 관련 파일 저장 완료]")
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=data verify files 저장 완료]")
      slack.sendMsg(flag, "검증 데이터 추출 완료", fileName)
    } catch {
      case ex: Exception => logger.error(s"[file=RunService] [function=main] [status=error] [message=${ex}]")
        slack.sendErrorMsg(flag, "검증 데이터 추출", fileName, ex.toString)
    } finally {
      logger.info(s"[file=RunService] [function=main] [status=end] [message=${ethDt} ${flag} 서비스를 종료합니다.]")
    }
  }
}
