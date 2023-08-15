/**
 * HJ 2차 분석 최종 결과 생성

 */
package sool.service.aggregate.agg_hj2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{concat_ws, lit, sum}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.functions.regexp_replace

class GetResHj2(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // HJ 2차 결과 마트
  def getMartResHj2(ethDt:String, anlysMartHj2:DataFrame, comMain:DataFrame) = {
    // 분모
    val denoDf = anlysMartHj2.
      groupBy('ADDR1).
      agg(sum("SUM_SUP_AMT").as("DENOM_AMT"), sum("CASE").as("DENOM_QT"))

    // 전체 분자
    val numeTotDf = anlysMartHj2.
      groupBy('ADDR1, 'COM_RGNO, 'MKT_CD).
      agg(sum("SUM_SUP_AMT").as("NUMER_TOTAL_AMT"), sum("CASE").as("NUMER_TOTAL_QT"))

    // HJ 2차 분자
    val numeHj2Df = anlysMartHj2.filter('MFG_NM === "HJ").
      groupBy('ADDR1, 'COM_RGNO, 'MKT_CD).
      agg(sum("SUM_SUP_AMT").as("NUMER_HJ_AMT"), sum("CASE").as("NUMER_HJ_QT"))

    // 분모, 분자 통합
    val totDf = denoDf.
      join(numeTotDf, Seq("ADDR1")).
      join(numeHj2Df, Seq("ADDR1", "COM_RGNO", "MKT_CD"), "left_outer").
      na.fill(0.0)

    // 컬럼 추가
    val totDfAddCol1 = totDf.withColumn("YM", lit(ethDt))
    val totDfAddCol2 = totDfAddCol1.withColumn("QT_TERRITORY", 'DENOM_QT.cast(DecimalType(18,2)))
    val totDfAddCol3 = totDfAddCol2.withColumn("AMT_TERRITORY", 'DENOM_AMT.cast(DecimalType(18,2)))
    val totDfAddCol4 = totDfAddCol3.withColumn("QT_RETAILER_RATIO", ('NUMER_TOTAL_QT / 'DENOM_QT) * 100)
    val totDfAddCol5 = totDfAddCol4.withColumn("AMT_RETAILER_RATIO", ('NUMER_TOTAL_AMT / 'DENOM_AMT) * 100)
    val totDfAddCol6 = totDfAddCol5.withColumn("QT_HJ_RATIO", ('NUMER_HJ_QT / 'DENOM_QT) * 100)
    val totDfAddCol7 = totDfAddCol6.withColumn("AMT_HJ_RATIO", ('NUMER_HJ_AMT / 'DENOM_AMT) * 100)
    val totDfAddColRes = totDfAddCol7.drop("ADDR1")

    // HJ 2차 결과 마트
    val martResHj2 = totDfAddColRes.join(comMain, Seq("COM_RGNO"))
    martResHj2
  }

  // HJ 2차 결과 마트에서 업체 정보 추출
  def getComResHj2(martResHj2:DataFrame) = {
    val martResHj2AddCol = martResHj2.
      withColumn("ADDR", concat_ws(" ", 'ADDR1, 'ADDR2, 'ADDR3D, 'DNUM, 'ADDR3R, 'RNUM, 'POST_CD)).
      withColumn("COM_NM",  regexp_replace($"COM_NM", "=", ""))

    val comResHj2 = martResHj2AddCol.select(
      'YM, 'COM_RGNO.as("VAT_NO"), 'COM_NM.as("RETAILER_NAME"),
      'ADDR.as("ADDRESS"), 'ADDR1.as("LEVEL1"), 'ADDR2.as("LEVEL2"),
      'ADDR3D.as("LEVEL3"), 'POST_CD, 'LEN.as("ADDRESS_CNT"),
      'ADDR1.as("TERRITORY"), 'QT_TERRITORY, 'AMT_TERRITORY
    ).distinct
    comResHj2
  }

  // HJ 2차 분석 결과
  def getAnlysResHj2(martResHj2:DataFrame) = {
    val anlysResHj2 = martResHj2.select(
      'YM, 'COM_RGNO.as("VAT_NO"), 'MKT_CD.as("MARKET_CODE"),
      'QT_RETAILER_RATIO, 'AMT_RETAILER_RATIO, 'QT_HJ_RATIO, 'AMT_HJ_RATIO
    )
    anlysResHj2
  }
}
