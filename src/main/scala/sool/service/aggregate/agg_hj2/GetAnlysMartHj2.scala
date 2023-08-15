/**
 * HJ 2차 분석 마트 생성

 */
package sool.service.aggregate.agg_hj2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

class GetAnlysMartHj2 (spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // HJ 2차 맥주 + 소주 마트 생성
  def getMartHj2(martHjb2:DataFrame, martHjs2:DataFrame) = {
    val martHjb2Sel = martHjb2.select('COM_RGNO, 'SUM_QT, 'SUM_SUP_AMT,
      'MFG_NM, 'MKT_NM, 'MKT_CD, 'ADDR1)
    val martHjs2Sel = martHjs2.select('COM_RGNO, 'SUM_QT, 'SUM_SUP_AMT,
      'MFG_NM, 'MKT_NM, 'MKT_CD, 'ADDR1)
    val martHj2 = martHjb2Sel.union(martHjs2Sel)
    martHj2
  }

  // HJ 2차 분석 마트
  def getAnlysMartHj2(comRgnoHj2:DataFrame, martHjb2:DataFrame, martHjs2:DataFrame) = {
    // HJ 2차 맥주 + 소주 마트
    val martHj2 = getMartHj2(martHjb2, martHjs2)

    val comRgnoMartHj2 = comRgnoHj2.join(martHj2, Seq("COM_RGNO"))

    // CASE 계산
    val caseUdf = udf((MKT_CD:String, sumQt:Double) => {
      if (MKT_CD.contains("S")) sumQt / 10800.0
      else sumQt / 10000.0
    })

    // HJ 2차 분석 마트
    val anlysMartHj2 = comRgnoMartHj2.withColumn("CASE", caseUdf('MKT_CD, 'SUM_QT) )
    anlysMartHj2
  }
}
