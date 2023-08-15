package sool.service.run

import sool.common.check_args.{CheckArgsRun, CheckDt, CheckIp}
import sool.common.function.{CmnFunc, GetTime, Slack}
import sool.service.item_tagging.{NonClassifiedTag, PreTaggedFile, UpdateCumulDict}

object RunSaveTag {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val checkArgsRunCls = new CheckArgsRun()
  val checkDtCls = new CheckDt()
  val checkIpCls = new CheckIp()
  val taggingRst = new PreTaggedFile(spark)
  val updateCumul = new UpdateCumulDict(spark)
  val reTagData = new NonClassifiedTag(spark)
  val slack = new Slack()


  // 메인
  def main(args: Array[String]): Unit = {
    val (ethDt, flag) = (args(0), args(1))
    val fileName = this.getClass.getSimpleName.stripSuffix("$")
    logger.info(s"[file=RunService] [function=main] [status=start] [message=${ethDt} ${flag} item tagging 파일 저장 시작합니다.]")

    if (flag == "dti") {
      try {
        val (checkIntCntInEthDtBool, checkYMFrmtBool) = checkArgsRunCls.checkServiceArgsVals(ethDt)
        val checkIp = checkIpCls.checkIp()
        if (checkIntCntInEthDtBool == false || checkYMFrmtBool == false || checkIp == false) {
          logger.error(s"[file=RunService] [function=main] [status=error] [message=집계연월 및 IP 를 확인하세요.]")
          return
        }
        checkDtCls.checkAggYM(ethDt) // 현재 연월, 예상 집계 연월, 실행 집계 연월 info 제공
        /* run */
        slack.sendMsg(flag, "아이템 매칭 테이블 업데이트, 지역별 재매칭, basetagged저장 시작", fileName)
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=item tagging file 저장 시작]")
        var resultStatus = taggingRst.runPreTagged(ethDt, flag)
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=item tagging file 저장 완료]")
        if (resultStatus == "fail") {
          logger.error(s"[file=RunService] [function=main] [status=error] [message=item_info_FINAL 파일을 확인하세요.]")
          slack.sendErrorMsg(flag, "아이템 매칭 테이블 업데이트, 지역별 재매칭, basetagged저장", fileName, "item_info_FINAL 파일을 확인하세요.")
          return
        }
        else {
          logger.info(s"[file=RunExtract] [functio=main] [status=running] [message=누적 업데이트 저장 시작]")
          resultStatus = updateCumul.runUpdateCumulDict(ethDt, flag)
          logger.info(s"[file=RunExtract] [function=main] [status=running] [message=누적 업데이트 저장 완료]")
          if (resultStatus == "fail") {
            logger.error(s"[file=RunService] [function=main] [status=error] [message=누적사전 관련 파일들을 확인하세요.(cumul_nminfo_df, final_nminfo_df)]")
            slack.sendErrorMsg(flag, "아이템 매칭 테이블 업데이트, 지역별 재매칭, basetagged저장", fileName, "누적사전 관련 파일들을 확인하세요.(cumul_nminfo_df, final_nminfo_df)")
            return
          }
          else {
            logger.info(s"[file=RunExtract] [function=main] [status=running] [message=지역별 소주 재매칭 시작]")
            reTagData.runItemReMat(ethDt, flag)
            logger.info(s"[file=RunExtract] [function=main] [status=running] [message=지역별 소주 재매칭 완료]")
            slack.sendMsg(flag, "아이템 매칭 테이블 업데이트, 지역별 재매칭, basetagged저장 완료", fileName)
          }
        }
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=base_tagged저장 완료]")
      } catch {
        case ex: Exception => logger.error(s"[file=RunService] [function=main] [status=error] [message=${ex}]")
          slack.sendErrorMsg(flag, "아이템 매칭 테이블 업데이트, 지역별 재매칭, basetagged저장", fileName, ex.toString())
      } finally {
        logger.info(s"[file=RunService] [function=main] [status=end] [message=${ethDt} ${flag} 서비스를 종료합니다.]")
      }
    }
    else if (flag == "inv") {
      try {
        val (checkIntCntInEthDtBool, checkYMFrmtBool) = checkArgsRunCls.checkServiceArgsVals(ethDt)
        val checkIp = checkIpCls.checkIp()
        if (checkIntCntInEthDtBool == false || checkYMFrmtBool == false || checkIp == false) {
          logger.error(s"[file=RunService] [function=main] [status=error] [message=집계연월 및 IP 를 확인하세요.]")
          return
        }
        checkDtCls.checkAggYM(ethDt) // 현재 연월, 예상 집계 연월, 실행 집계 연월 info 제공
        /* run */
        slack.sendMsg(flag, "지역별 재매칭, basetagged저장 시작", fileName)
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=item tagging file 저장 시작]")
        reTagData.runItemReMat(ethDt, flag)
        logger.info(s"[file=RunExtract] [function=main] [status=running] [message=item tagging file 저장 완료]")
        slack.sendMsg(flag, "지역별 재매칭, basetagged저장 완료", fileName)
      }
      catch {
        case ex: Exception => logger.error(s"[file=RunService] [function=main] [status=error] [message=${ex}]")
          slack.sendErrorMsg(flag, "아이템 매칭 테이블 업데이트, 지역별 재매칭, basetagged저장", fileName, ex.toString)
      }
      finally {
        logger.info(s"[file=RunService] [function=main] [status=end] [message=${ethDt} ${flag} 서비스를 종료합니다.]")
      }
    }
    else {
      logger.error(s"[file=RunService] [function=main] [status=error] [message=집계연월 및 인자값을 확인하세요.]")
    }
  }
}
