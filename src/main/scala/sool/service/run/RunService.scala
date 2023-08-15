/**
 * 주류 마트 생성 및 집계 실행 코드

 */
package sool.service.run

import sool.service.aggregate.agg_hj.RunAggHj
import sool.service.aggregate.agg_hj2.RunAggHj2
import sool.service.aggregate.agg_hk.RunAggHk
import sool.service.get_marts.get_mart_dk.RunGetMartDk
import sool.service.get_marts.get_mart_hj.RunGetMartHj
import sool.service.get_marts.get_mart_hk.RunGetMartHk
import sool.common.check_args.{CheckArgsRun, CheckDt, CheckIp}
import sool.common.function.{CmnFunc, GetTime, Slack}
import sool.extract.extract_data.RunExtrctDtiEthanol
import sool.service.aggregate.agg_anglnt.RunAggAnglNt
import sool.service.aggregate.agg_dk.RunAggDk
import sool.service.aggregate.agg_hj3.RunAggHj3

object RunService {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val checkArgsRunCls = new CheckArgsRun()
  val checkDtCls = new CheckDt()
  val checkIpCls = new CheckIp()
  val runGetMartHjCls = new RunGetMartHj(spark)
  val runGetMartDkCls = new RunGetMartDk(spark)
  val runGetMartHkCls = new RunGetMartHk(spark)
  val runAggHjCls = new RunAggHj(spark)
  val runAggHj2Cls = new RunAggHj2(spark)
  val runAggHj3Cls = new RunAggHj3(spark)
  val runAggDkCls = new RunAggDk(spark)
  val runAggHkCls = new RunAggHk(spark)
  val runAggAnglNtCls = new RunAggAnglNt(spark)
  val slack = new Slack()

  val fileName = this.getClass.getSimpleName.stripSuffix("$")
  // 고객사별 마트 생성
  def runGetMarts(ethDt: String, opt2: String, flag: String) = {
    if (opt2 == "hj") {
      slack.sendMsg(flag, "HJ 마트 생성 시작", fileName)
      runGetMartHjCls.runGetMartHj(ethDt, flag) // HJ 마트 생성
      slack.sendMsg(flag, "HJ 마트 생성 완료", fileName)
    } else if (opt2 == "dk") {
      slack.sendMsg(flag, "DK 마트 생성 시작", fileName)
      runGetMartDkCls.runGetMartDk(ethDt, flag) // DK 마트 생성
      slack.sendMsg(flag, "DK 마트 생성 완료", fileName)
    } else if (opt2 == "hk") {
      slack.sendMsg(flag, "HK 마트 생성 시작", fileName)
      runGetMartHkCls.runGetMartHk(ethDt, flag) // HK 마트 생성
      slack.sendMsg(flag, "HK 마트 생성 완료", fileName)
    } else if (opt2 == "all") {
      slack.sendMsg(flag, "HJ 마트 생성 시작", fileName)
      runGetMartHjCls.runGetMartHj(ethDt, flag) // HJ 마트 생성
      slack.sendMsg(flag, "HJ 마트 생성 완료", fileName)
      slack.sendMsg(flag, "DK 마트 생성 시작", fileName)
      runGetMartDkCls.runGetMartDk(ethDt, flag) // DK 마트 생성
      slack.sendMsg(flag, "DK 마트 생성 완료", fileName)
      slack.sendMsg(flag, "HK 마트 생성 시작", fileName)
      runGetMartHkCls.runGetMartHk(ethDt, flag) // HK 마트 생성
      slack.sendMsg(flag, "HK 마트 생성 완료", fileName)
    } else {
      logger.error(s"[file=RunService] [function=runGetMarts] [status=error] [message=고객사명이 잘못되었습니다.]")
      slack.sendErrorMsg(flag, "마트 생성 에러", fileName, "고객사명이 잘못되었습니다.")
    }
  }

  // 고객사별 집계 결과 생성
  def runAgg(ethDt: String, opt2: String, flag: String) = {
    if (opt2 == "hj") {
      slack.sendMsg(flag, "HJ 집계 결과 생성 시작", fileName)
      runAggHjCls.runAggHj(ethDt, flag)  // HJ 집계 결과 생성 및 저장
      slack.sendMsg(flag, "HJ 집계 결과 생성 완료", fileName)
      if (flag == "inv"){
        slack.sendMsg(flag, "INV 4만개 데이터 생성 시작", fileName)
        runAggHj2Cls.runAggInv2(ethDt, flag)   // HJ2차 집계 결과 생성 및 저장
        slack.sendMsg(flag, "INV 4만개 데이터 생성 완료", fileName)
      }
    } else if (opt2 == "hj2") {
      slack.sendMsg(flag, "HJ2차 집계 결과 생성 시작", fileName)
      runAggHj2Cls.runAggHj2(ethDt, flag)   // HJ2차 집계 결과 생성 및 저장
      slack.sendMsg(flag, "HJ2차 집계 결과 생성 완료", fileName)
    } else if (opt2 == "hj3") {
      slack.sendMsg(flag, "HJ3차 집계 결과 생성 시작", fileName)
      runAggHj3Cls.runAggHj3(ethDt, flag)   // HJ3차 부산, 수도권 집계 결과 생성 및 저장
      slack.sendMsg(flag, "HJ3차 집계 결과 생성 완료", fileName)
    } else if (opt2 == "dk") {
      slack.sendMsg(flag, "DK 집계 결과 생성 시작", fileName)
      runAggDkCls.runAggDk(ethDt, flag)  // DK 집계 결과 생성 및 저장
      slack.sendMsg(flag, "DK 집계 결과 생성 완료", fileName)
    } else if (opt2 == "hk") {
      slack.sendMsg(flag, "HK 집계 결과 생성 시작", fileName)
      runAggHkCls.runAggHk(ethDt, flag)  // HK 집계 결과 생성 및 저장
      slack.sendMsg(flag, "HK 집계 결과 생성 완료", fileName)
    } else if (opt2 == "anglnt") {
      slack.sendMsg(flag, "anglnt 집계 결과 생성 시작", fileName)
      runAggAnglNtCls.runAggAnglNt(ethDt, flag)   // AG 집계 결과 생성 및 DB 저장
      slack.sendMsg(flag, "anglnt 집계 결과 생성 완료", fileName)
    } else if (opt2 == "all") {   // 이 순서로 실행해야 생성되는 집계 결과들 순으로 작업이 편하다.
      slack.sendMsg(flag, "DK 집계 결과 생성 시작", fileName)
      runAggDkCls.runAggDk(ethDt, flag)   // DK 집계 결과 생성 및 저장
      slack.sendMsg(flag, "DK 집계 결과 생성 완료", fileName)
      slack.sendMsg(flag, "HJ 집계 결과 생성 시작", fileName)
      runAggHjCls.runAggHj(ethDt, flag)  // HJ 집계 결과 생성 및 저장
      slack.sendMsg(flag, "HJ 집계 결과 생성 시작", fileName)
      slack.sendMsg(flag, "HJ2차 집계 결과 생성 시작", fileName)
      runAggHj2Cls.runAggHj2(ethDt, flag)   // HJ2차 집계 결과 생성 및 저장
      slack.sendMsg(flag, "HJ2차 집계 결과 생성 시작", fileName)
      slack.sendMsg(flag, "HJ3차 집계 결과 생성 시작", fileName)
      runAggHj3Cls.runAggHj3(ethDt, flag)
      slack.sendMsg(flag, "HJ3차 집계 결과 생성 시작", fileName)
      slack.sendMsg(flag, "HK 집계 결과 생성 시작", fileName)
      runAggHkCls.runAggHk(ethDt, flag)  // HK 집계 결과 생성 및 저장
      slack.sendMsg(flag, "HK 집계 결과 생성 시작", fileName)
      slack.sendMsg(flag, "anglnt 집계 결과 생성 시작", fileName)
      runAggAnglNtCls.runAggAnglNt(ethDt, flag)   // AG 집계 결과 생성 및 DB 저장\
      slack.sendMsg(flag, "anglnt 집계 결과 생성 시작", fileName)
    } else {
      logger.error(s"[file=RunService] [function=runAgg] [status=error] [message=고객사명이 잘못되었습니다.]")
      slack.sendErrorMsg(flag, "집계 생성 에러", fileName, "고객사명이 잘못되었습니다.")
    }
  }

  // 메인
  def main(args: Array[String]): Unit = {
    // args(0): 집계연월, args(1): 마트를 생성할 건지, 집계 결과를 생성할 건지, args(2): 고객사명
    val (ethDt, opt1, opt2, flag) = (args(0), args(1), args(2), args(3))
    logger.info(s"[file=RunService] [function=main] [status=start] [message=${ethDt} ${opt1} ${opt2} ${flag} 서비스를 시작합니다.]")

    try {
      val (checkIntCntInEthDtBool, checkYMFrmtBool) = checkArgsRunCls.checkServiceArgsVals(ethDt)
      val checkIp = checkIpCls.checkIp()
      if (checkIntCntInEthDtBool == false || checkYMFrmtBool == false || checkIp == false) {
        logger.error(s"[file=RunService] [function=main] [status=error] [message=집계연월 및 IP 를 확인하세요.]")
        return
      }
      checkDtCls.checkAggYM(ethDt) // 현재 연월, 예상 집계 연월, 실행 집계 연월 info 제공
      if (opt1 == "mart") {
        runGetMarts(ethDt, opt2, flag)  // 고객사별 마트 생성
      } else if (opt1 == "agg") {
        runAgg(ethDt, opt2, flag)   // 고객사별 집계 결과 생성 및 저장
      } else {
        logger.error(s"[file=RunService] [function=main] [status=error] [message=mart, agg 중 하나를 입력하세요.]")
      }
    } catch {
      case ex: Exception => logger.error(s"[file=RunService] [function=main] [status=error] [message=${ex}]")
        slack.sendErrorMsg(flag, "에러", fileName, ex.toString)
    } finally {
      logger.info(s"[file=RunService] [function=main] [status=end] [message=${ethDt} ${opt1} ${opt2} ${flag} 서비스를 종료합니다.]")
    }
  }
}