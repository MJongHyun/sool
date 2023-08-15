/**
 * HK 집계 분모 추출

 */
package sool.service.aggregate.agg_hk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, sum}

class GetDenoDfHk(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 1. 지역, 기타 단위 집계
  def getMarketHk(martHk:DataFrame) = {
    val slctCols = Seq("ADDR1", "ADDR2", "MARKET_VOL").map(col)

    // QT
    val marketQtG = martHk.
      groupBy('ADDR1, 'ADDR2).
      agg(sum("QT").as("MARKET_VOL"))
    val marketQt = marketQtG.
      select(slctCols:_*).
      withColumn("REMARK", lit("Quantity"))

    // AMT
    val marketAmtG = martHk.
      groupBy('ADDR1, 'ADDR2).
      agg(sum("SUP_AMT").as("MARKET_VOL"))
    val marketAmt = marketAmtG.
      select(slctCols:_*).
      withColumn("REMARK", lit("Amount"))

    val marketHk = marketQt.union(marketAmt)
    marketHk
  }

  // 2. 지역, 시장 카테고리(MARKET_CATEGORY) 단위 집계
  def getCateHk(martHk:DataFrame) = {
    // QT
    val cateQTG = martHk.
      groupBy('ADDR1, 'ADDR2, 'MKT_CTGY_NM).
      agg(sum("QT").as("CATE_VOL"))
    val cateQT = cateQTG.
      select('ADDR1, 'ADDR2, 'MKT_CTGY_NM.as("MARKET_CATEGORY"), 'CATE_VOL).
      withColumn("REMARK", lit("Quantity"))

    // AMT
    val cateAMTG = martHk.
      groupBy('ADDR1, 'ADDR2, 'MKT_CTGY_NM).
      agg(sum("SUP_AMT").as("CATE_VOL"))
    val cateAMT = cateAMTG.
      select('ADDR1, 'ADDR2, 'MKT_CTGY_NM.as("MARKET_CATEGORY"), 'CATE_VOL).
      withColumn("REMARK", lit("Amount"))

    val cateHk = cateQT.union(cateAMT)
    cateHk
  }

  // 분모 추출
  def getDenoDfHk(martHk:DataFrame) = {
    val denoMarketQtAmt = getMarketHk(martHk)
    val denoCateQtAmt = getCateHk(martHk)
    val denoCateTot = denoMarketQtAmt.
      withColumn("MARKET_CATEGORY", lit("Total")).
      select('ADDR1, 'ADDR2, 'MARKET_CATEGORY, 'MARKET_VOL.as("CATE_VOL"), 'REMARK)
    val denoCate = denoCateQtAmt.union(denoCateTot)
    val denoDfHk= denoMarketQtAmt.join(denoCate, Seq("ADDR1", "ADDR2", "REMARK"), "left_outer")
    denoDfHk
  }
}
