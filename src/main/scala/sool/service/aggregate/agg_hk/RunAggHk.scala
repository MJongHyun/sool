/**
 * HK 집계 결과 생성

 */
package sool.service.aggregate.agg_hk

import sool.service.run.RunService.logger
import sool.service.aggregate.agg_hk._

class RunAggHk(spark: org.apache.spark.sql.SparkSession) {
  // 집계에 필요한 클래스 선언
  val getAggDfsHkCls = new GetAggDfsHk(spark)
  val getDenoDfHkCls = new GetDenoDfHk(spark)
  val getNumeDfHkCls = new GetNumeDfHk(spark)
  val getMstrInfoHkCls = new GetMstrInfoHk(spark)
  val getResHkCls = new GetResHk(spark)
  val saveDfsHkCls = new SaveDfsHk(spark)

  def runAggHk(ethDt:String, flag: String) = {
    logger.info("[appName=sool] [function=runAggHk] [runStatus=start] [message=start]")

    // 데이터 준비
    val (martHk, mstrHk, branchDone) = getAggDfsHkCls.getAggDfsHk(ethDt, flag)

    // 분모
    val denoDfHk = getDenoDfHkCls.getDenoDfHk(martHk)
    saveDfsHkCls.saveDenoDfHk(denoDfHk, ethDt, flag)

    // 분자
    val numeDfHk = getNumeDfHkCls.getNumeDfHk(martHk)
    saveDfsHkCls.saveNumeDfHk(numeDfHk, ethDt, flag)

    // DB HK 마스터에서 BRAND, MARKET_CATEGORY, HK_CATEGORY 추출
    val mstrInfoHk = getMstrInfoHkCls.getMstrInfoHk(mstrHk)

    // 최종 HK 집계 결과
    val resHk = getResHkCls.getResHk(ethDt, denoDfHk, numeDfHk, mstrInfoHk, branchDone)
    saveDfsHkCls.saveResHk(resHk, ethDt, flag)

    // 최종 HK 집계 결과 저장
    val (mktTot, mktDra, mktNor, mktOth) = getResHkCls.getFinalResHk(resHk)
    saveDfsHkCls.saveResHkXlsx(mktTot, mktDra, mktNor, mktOth, ethDt, flag)

    logger.info("[appName=sool] [function=runAggHk] [runStatus=end] [message=end]")
  }
}