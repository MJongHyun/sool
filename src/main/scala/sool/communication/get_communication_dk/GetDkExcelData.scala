package sool.communication.get_communication_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, regexp_replace}

class GetDkExcelData (spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  def getDkWskjItemExcelData(dkWskItemRaw: DataFrame) = {
    val dkIntNo = dkWskItemRaw.
      withColumn("intNo", regexp_replace('WSK_ID, "DKW", "") cast "Int")
    val dkIntNoW = Window.orderBy('intNo.asc)
    val dkNo = dkIntNo.
      withColumn("NO", rank().over(dkIntNoW))
    val dkWskExcelData = dkNo.drop('intNo)
      .orderBy('NO)

    dkWskExcelData
  }

  def getDkBrItemExcelData(dkBrItemRaw: DataFrame) = {
    val dkIntNo = dkBrItemRaw.withColumn("intNo", regexp_replace('BR_ID, "DKB", "") cast "Int")
    val dkIntNoW = Window.orderBy('intNo.asc)
    val dkNo = dkIntNo.
      withColumn("NO", rank().over(dkIntNoW))
    val dkBrExcelData = dkNo.drop('intNo)
      .orderBy('NO)

    dkBrExcelData
  }

  def getDkMnExcelData(dkMnRaw: DataFrame, dkFltrRaw: DataFrame) = {
    val dkMnMarket = dkMnRaw.join(dkFltrRaw, 'FLTR_ID1 === 'FLTR_ID).
      select('MN_ID,
        'MN_NM,
        'RSLT_YN,
        'TOP_MN_NM,
        'FLTR_DESC_TXT as "COLUMN_MARKET",
        'FLTR_ID2,
        'MN_DESC_TXT)

    val dkMnMarketDK = dkMnMarket.join(dkFltrRaw, 'FLTR_ID2 === 'FLTR_ID).
      select('MN_ID,
        'MN_NM,
        'RSLT_YN,
        'TOP_MN_NM,
        'COLUMN_MARKET,
        'FLTR_DESC_TXT as "COLUMN_DK",
        'MN_DESC_TXT)

    dkMnMarketDK
  }
}