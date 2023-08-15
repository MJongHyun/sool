package sool.communication.get_communication_hj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank, regexp_replace}

class GetHjExcelData (spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  def getHjSjItemExcelData(hjSjItemRaw: DataFrame) = {
    val hjIntNo = hjSjItemRaw.withColumn("intNo", regexp_replace('SJ_ID, "HJS", "") cast "Int")
    val hjIntNoW = Window.orderBy('intNo.asc)
    val hjNo = hjIntNo.
      withColumn("NO", rank().over(hjIntNoW))
    val hjSjExcelData = hjNo.drop('intNo)
      .orderBy('NO)

    hjSjExcelData
  }

  def getHjBrItemExcelData(hjBrItemRaw: DataFrame) = {
    val hjIntNo = hjBrItemRaw.withColumn("intNo", regexp_replace('BR_ID, "HJB", "") cast "Int")
    val hjIntNoW = Window.orderBy('intNo.asc)
    val hjNo = hjIntNo.
      withColumn("NO", rank().over(hjIntNoW))
    val hjBrExcelData = hjNo.drop('intNo)
      .orderBy('NO)

    hjBrExcelData
  }

  def getHjMnExcelData(hjMnRaw: DataFrame, hjFltrRaw: DataFrame) = {
    val hjMnNumer = hjMnRaw.join(hjFltrRaw, 'FLTR_ID2 === 'FLTR_ID).
      select('CRTN_YM,
        'MN_ID,
        'MN_NM,
        'FLTR_DESC_TXT as "NUMER_NM",
        'FLTR_ID as "NUMER_CD",
        'DESC_TXT as "NUMER_DEF",
        'FLTR_ID1
      )

    val hjMnNumerDenom = hjMnNumer.join(hjFltrRaw, 'FLTR_ID1 === 'FLTR_ID).
      select('CRTN_YM,
        'MN_ID,
        'MN_NM,
        'NUMER_NM,
        'NUMER_CD,
        'NUMER_DEF,
        'FLTR_DESC_TXT as "DENOM_NM",
        'FLTR_ID as "DENOM_CD",
        'DESC_TXT as "DENOM_DEF"
      )

    hjMnNumerDenom
  }

  def getHj2ItemData(hj2MnRaw: DataFrame, hj2ItemRaw: DataFrame) = {
    val hj2SelectItem = hj2MnRaw.join(hj2ItemRaw, Seq("MKT_CD")).
      select('ITEM_NM, 'MKT_NM)

    hj2SelectItem
  }
}