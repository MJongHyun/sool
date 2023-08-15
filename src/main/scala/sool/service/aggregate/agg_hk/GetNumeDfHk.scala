/**
 * HK 집계 분자 추출

 */
package sool.service.aggregate.agg_hk

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, sum}

class GetNumeDfHk(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // BRAND : Total, MARKET_CATEGORY : Total, HK_CATEGORY : Total
  def getHkTotQtAmt(martHkFil:DataFrame, slctCol:Seq[Column]) = {
    // QT
    val hkTotQtG = martHkFil.
      groupBy('ADDR1, 'ADDR2).
      agg(sum("QT").as("BRAND_VAL"))
    val hkTotQt = hkTotQtG.
      withColumn("BRAND", lit("Total")).
      withColumn("MARKET_CATEGORY", lit("Total")).
      withColumn("HNK_CATEGORY", lit("Total")).
      withColumn("REMARK", lit("Quantity")).
      select(slctCol:_*)

    // AMT
    val hkTotAmtG = martHkFil.
      groupBy('ADDR1, 'ADDR2).
      agg(sum("SUP_AMT").as("BRAND_VAL"))
    val hkTotAmt = hkTotAmtG.
      withColumn("BRAND", lit("Total")).
      withColumn("MARKET_CATEGORY", lit("Total")).
      withColumn("HNK_CATEGORY", lit("Total")).
      withColumn("REMARK", lit("Amount")).
      select(slctCol:_*)
    (hkTotQt, hkTotAmt)
  }

  // BRAND : 브랜드별, MARKET_CATEGORY : Total, HNK_CATEGORY : Total
  def getBrTotQtAmt(martHkFil:DataFrame, slctCol:Seq[Column]) = {
    // QT
    val brTotQtG = martHkFil.
      groupBy('ADDR1, 'ADDR2, 'BRND_NM).
      agg(sum("QT").as("BRAND_VAL"))
    val brTotQt = brTotQtG.
      withColumn("MARKET_CATEGORY", lit("Total")).
      withColumn("HNK_CATEGORY", lit("Total")).
      withColumn("REMARK", lit("Quantity")).
      withColumnRenamed("BRND_NM", "BRAND").
      select(slctCol:_*)

    // AMT
    val brTotAmtG = martHkFil.
      groupBy('ADDR1, 'ADDR2, 'BRND_NM).
      agg(sum("SUP_AMT").as("BRAND_VAL"))
    val brTotAmt = brTotAmtG.
      withColumn("MARKET_CATEGORY", lit("Total")).
      withColumn("HNK_CATEGORY", lit("Total")).
      withColumn("REMARK", lit("Amount")).
      withColumnRenamed("BRND_NM", "BRAND").
      select(slctCol:_*)
    (brTotQt, brTotAmt)
  }

  // BRAND : 브랜드별, MARKET_CATEGORY : Total, HNK_CATEGORY : HK 카테고리별
  def getBrHcTotQtAmt(martHkFil:DataFrame, slctCol:Seq[Column]) = {
    // QT
    val brHcTotQtG = martHkFil.
      groupBy('ADDR1, 'ADDR2, 'BRND_NM, 'HK_CTGY_NM).
      agg(sum("QT").as("BRAND_VAL"))
    val brHcTotQt = brHcTotQtG.
      withColumn("MARKET_CATEGORY", lit("Total")).
      withColumn("REMARK", lit("Quantity")).
      withColumnRenamed("BRND_NM", "BRAND").
      withColumnRenamed("HK_CTGY_NM", "HNK_CATEGORY").
      select(slctCol:_*)

    // AMT
    val brHcTotAmtG = martHkFil.
      groupBy('ADDR1, 'ADDR2, 'BRND_NM, 'HK_CTGY_NM).
      agg(sum("SUP_AMT").as("BRAND_VAL"))
    val brHcTotAmt = brHcTotAmtG.
      withColumn("MARKET_CATEGORY", lit("Total")).
      withColumn("REMARK", lit("Amount")).
      withColumnRenamed("BRND_NM", "BRAND").
      withColumnRenamed("HK_CTGY_NM", "HNK_CATEGORY").
      select(slctCol:_*)
    (brHcTotQt, brHcTotAmt)
  }

  // BRAND : 브랜드별, MARKET_CATEGORY : 마켓 카테고리별, HNK_CATEGORY : Total
  def getBrMkTotQtAmt(martHkFil:DataFrame, slctCol:Seq[Column]) = {
    // QT
    val brMkTotQtG = martHkFil.
      groupBy('ADDR1, 'ADDR2, 'BRND_NM, 'MKT_CTGY_NM).
      agg(sum("QT").as("BRAND_VAL"))
    val bkMkTotQt = brMkTotQtG.
      withColumn("HNK_CATEGORY", lit("Total")).
      withColumn("REMARK", lit("Quantity")).
      withColumnRenamed("BRND_NM", "BRAND").
      withColumnRenamed("MKT_CTGY_NM", "MARKET_CATEGORY").
      select(slctCol:_*)

    // AMT
    val brMkTotAmtG = martHkFil.
      groupBy('ADDR1, 'ADDR2, 'BRND_NM, 'MKT_CTGY_NM).
      agg(sum("SUP_AMT").as("BRAND_VAL"))
    val brMkTotAmt = brMkTotAmtG.
      withColumn("HNK_CATEGORY", lit("Total")).
      withColumn("REMARK", lit("Amount")).
      withColumnRenamed("BRND_NM", "BRAND").
      withColumnRenamed("MKT_CTGY_NM", "MARKET_CATEGORY").
      select(slctCol:_*)
    (bkMkTotQt, brMkTotAmt)
  }

  // BRAND : 브랜드별, MARKET_CATEGORY : 마켓 카테고리별, HNK_CATEGORY : 하이네켓 카테고리별
  def getBrMkHkTotQtAmt(martHkFil:DataFrame, slctCol:Seq[Column]) = {
    // QT
    val brMkHkTotQtG = martHkFil.
      groupBy('ADDR1, 'ADDR2, 'BRND_NM, 'MKT_CTGY_NM, 'HK_CTGY_NM).
      agg(sum("QT").as("BRAND_VAL"))
    val brMkHkTotQt = brMkHkTotQtG.
      withColumn("REMARK", lit("Quantity")).
      withColumnRenamed("BRND_NM", "BRAND").
      withColumnRenamed("MKT_CTGY_NM", "MARKET_CATEGORY").
      withColumnRenamed("HK_CTGY_NM", "HNK_CATEGORY").
      select(slctCol:_*)

    // AMT
    val brMkHkTotAmtG = martHkFil.
      groupBy('ADDR1, 'ADDR2, 'BRND_NM, 'MKT_CTGY_NM, 'HK_CTGY_NM).
      agg(sum("SUP_AMT").as("BRAND_VAL"))
    val brMkHkTotAmt = brMkHkTotAmtG.
      withColumn("REMARK", lit("Amount")).
      withColumnRenamed("BRND_NM", "BRAND").
      withColumnRenamed("MKT_CTGY_NM", "MARKET_CATEGORY").
      withColumnRenamed("HK_CTGY_NM", "HNK_CATEGORY").
      select(slctCol:_*)
    (brMkHkTotQt, brMkHkTotAmt)
  }

  def getNumeDfHk(martHk:DataFrame) = {
    val martHkFil = martHk.filter('BRND_NM.isNotNull)
    val slctCol = Seq("ADDR1", "ADDR2", "BRAND", "MARKET_CATEGORY", "HNK_CATEGORY", "BRAND_VAL", "REMARK").map(col)

    // 각 조건별 분자 데이터 프레임
    val (hkTotQt, hkTotAmt) = getHkTotQtAmt(martHkFil, slctCol)
    val (brTotQt, brTotAmt) = getBrTotQtAmt(martHkFil, slctCol)
    val (brHcTotQt, brHcTotAmt) = getBrHcTotQtAmt(martHkFil, slctCol)
    val (bkMkTotQt, brMkTotAmt) = getBrMkTotQtAmt(martHkFil, slctCol)
    val (brMkHkTotQt, brMkHkTotAmt) = getBrMkHkTotQtAmt(martHkFil, slctCol)

    // 분자 데이터 프레임 합치기
    val numeDfs = Seq(hkTotQt, hkTotAmt, brTotQt, brTotAmt, brHcTotQt,
      brHcTotAmt, bkMkTotQt, brMkTotAmt, brMkHkTotQt, brMkHkTotAmt)
    val numeDfHk = numeDfs.reduce(_ union _)
    numeDfHk
  }
}
