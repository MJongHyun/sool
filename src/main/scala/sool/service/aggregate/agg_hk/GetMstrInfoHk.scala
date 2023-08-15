/**
 * DB HK 마스터에서 BRAND, MARKET_CATEGORY, HNK_CATEGORY 추출

 */
package sool.service.aggregate.agg_hk

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, lit}

class GetMstrInfoHk(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // BRAND : Total, MARKET_CATEGORY : Total, HK_CATEGORY : Total
  def getHkTotInfo(orderCols:Seq[Column]) = {
    val hkTotInfo = Seq(("Total", "Total", "Total")).
      toDF("MARKET_CATEGORY", "HNK_CATEGORY", "BRAND").
      select(orderCols:_*)
    hkTotInfo
  }

  // BRAND : 브랜드별, MARKET_CATEGORY : Total, HNK_CATEGORY : Total
  def getBrTotInfo(mstrHkFil:DataFrame, orderCols:Seq[Column]) = {
    val brTotInfoPre = mstrHkFil.select('BRND_NM.as("BRAND")).distinct
    val brTotInfo = brTotInfoPre.
      withColumn("HNK_CATEGORY", lit("Total")).
      withColumn("MARKET_CATEGORY", lit("Total")).
      select(orderCols:_*)
    brTotInfo
  }

  // BRAND : 브랜드별, MARKET_CATEGORY : Total, HNK_CATEGORY : HK 카테고리별
  def getBrHcTotInfo(mstrHkFil:DataFrame, orderCols:Seq[Column]) = {
    val brHcTotInfoPre = mstrHkFil.
      select('BRND_NM.as("BRAND"), 'HK_CTGY_NM.as("HNK_CATEGORY")).distinct
    val brHcTotInfo = brHcTotInfoPre.
      withColumn("MARKET_CATEGORY", lit("Total")).
      select(orderCols:_*)
    brHcTotInfo
  }

  // BRAND : 브랜드별, MARKET_CATEGORY : 마켓 카테고리별, HNK_CATEGORY : Total
  def getBrMkTotInfo(mstrHkFil:DataFrame, orderCols:Seq[Column]) = {
    val brMkTotInfoPre = mstrHkFil.
      select('BRND_NM.as("BRAND"), 'MKT_CTGY_NM.as("MARKET_CATEGORY")).distinct
    val brMkTotInfo = brMkTotInfoPre.
      withColumn("HNK_CATEGORY", lit("Total")).
      select(orderCols:_*)
    brMkTotInfo
  }

  // BRAND : 브랜드별, MARKET_CATEGORY : 마켓 카테고리별, HNK_CATEGORY : 하이네켓 카테고리별
  def getBrMkHkTotInfo(mstrHkFil:DataFrame, orderCols:Seq[Column]) = {
    val brMkHkTotInfoPre = mstrHkFil.select(
      'MKT_CTGY_NM.as("MARKET_CATEGORY"),
      'HK_CTGY_NM.as("HNK_CATEGORY"),
      'BRND_NM.as("BRAND")
    ).distinct
    val brMkHkTotInfo = brMkHkTotInfoPre.select(orderCols:_*)
    brMkHkTotInfo
  }

  def getMstrInfoHk(mstrHk:DataFrame) = {
    val mstrHkFil = mstrHk.filter('BRND_NM.isNotNull)
    val orderCols = Seq("MARKET_CATEGORY", "HNK_CATEGORY", "BRAND").map(col)

    val hkTotInfo = getHkTotInfo(orderCols)
    val brTotInfo = getBrTotInfo(mstrHkFil, orderCols)
    val brHcTotInfo = getBrHcTotInfo(mstrHkFil, orderCols)
    val brMkTotInfo = getBrMkTotInfo(mstrHkFil, orderCols)
    val brMkHkTotInfo = getBrMkHkTotInfo(mstrHkFil, orderCols)

    val mstrInfoHkDfs = Seq(hkTotInfo, brTotInfo, brHcTotInfo, brMkTotInfo, brMkHkTotInfo)
    val mstrInfoHk = mstrInfoHkDfs.reduce(_ union _)
    mstrInfoHk
  }
}
