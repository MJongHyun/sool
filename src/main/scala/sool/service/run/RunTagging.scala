/**
 * 아이템 모델성 관련 train, test file 생

 */
package sool.service.run

import sool.common.check_args.{CheckArgsRun, CheckDt, CheckIp}
import sool.common.function.{CmnFunc, GetTime, Slack}
import sool.service.item_tagging.MakeMLTrainFile

object RunTagging {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val checkArgsRunCls = new CheckArgsRun()
  val checkDtCls = new CheckDt()
  val checkIpCls = new CheckIp()
  val itemTagging = new MakeMLTrainFile(spark)
  val slack = new Slack()

  // 메인
  def main(args: Array[String]): Unit = {
    val (ethDt, flag) = (args(0), args(1))
    val fileName = this.getClass.getSimpleName.stripSuffix("$")
    // args(0): 집계연월
    logger.info(s"[file=RunService] [function=main] [status=start] [message=${ethDt} ${flag} item tagging 파일 만들기를 시작합니다.]")

    try {
      val (checkIntCntInEthDtBool, checkYMFrmtBool) = checkArgsRunCls.checkServiceArgsVals(ethDt)
      val checkIp = checkIpCls.checkIp()
      if (checkIntCntInEthDtBool == false || checkYMFrmtBool == false || checkIp == false) {
        logger.error(s"[file=RunService] [function=main] [status=error] [message=집계연월 및 IP 를 확인하세요.]")
        slack.sendErrorMsg(flag, "아이템 매칭 파일 생성", fileName, "집계연월 및 IP 를 확인하세요.")
        return
      }
      checkDtCls.checkAggYM(ethDt) // 현재 연월, 예상 집계 연월, 실행 집계 연월 info 제공
      /* run */
      slack.sendMsg(flag, "아이템 매칭 파일 생성 시작", fileName)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=item tagging file 만들기 시작]")
      itemTagging.runGetMlTrainFiles(ethDt, flag)
      logger.info(s"[file=RunExtract] [function=main] [status=running] [message=item tagging file 만들기 완료]")
      slack.sendMsg(flag, "아이템 매칭 파일 생성 완료", fileName)
    } catch {
      case ex: Exception => logger.error(s"[file=RunService] [function=main] [status=error] [message=${ex}]")
        slack.sendErrorMsg(flag, "아이템 매칭 파일 생성 완료", fileName, ex.toString)
    } finally {
      logger.info(s"[file=RunService] [function=main] [status=end] [message=${ethDt} ${flag} 서비스를 종료합니다.]")
    }
  }
}