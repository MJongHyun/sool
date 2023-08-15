/**
 * HK 집계 결과 생성

 */
package sool.service.aggregate.agg_hk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class GetResHk(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // MS 산출
  def getResHk(ethDt:String,
               denoDfHk:DataFrame,
               numeDfHk:DataFrame,
               hkMstrInfo:DataFrame,
               branchDone:DataFrame) = {
    // 분자, 분모, HK 마스터 정보, 브랜치 주소 합치기
    val denoHmi = denoDfHk.join(hkMstrInfo, Seq("MARKET_CATEGORY"), "left_outer")
    val denoNumeHmi = denoHmi.join(numeDfHk, Seq("ADDR1", "ADDR2", "BRAND",
      "MARKET_CATEGORY", "HNK_CATEGORY", "REMARK"), "left_outer")
    val denoNumeHmiRfn = denoNumeHmi.na.fill(0.0, Seq("BRAND_VAL")).withColumn("DATE", lit(ethDt))
    val denoNumeHmiBd = denoNumeHmiRfn.join(branchDone, Seq("ADDR1", "ADDR2"), "left_outer")

    // MS 산출
    val resMs = denoNumeHmiBd.
      withColumn("MARKET_MS", ('BRAND_VAL / 'MARKET_VOL) * 100).
      withColumn("CATE_MS", ('BRAND_VAL / 'CATE_VOL) * 100).
      na.fill(0, Seq("MARKET_MS", "CATE_MS")) //분모가 0이 나와서 null 값이 나오는 경우 0으로 변환

    // HK MS 집계 결과
    val resHk = resMs.select(
      'DATE, 'Branch, 'Sigu, 'ADDR1, 'ADDR2,
      'BRAND, 'MARKET_CATEGORY, 'HNK_CATEGORY, 'REMARK, 'MARKET_VOL,
      'MARKET_MS, 'CATE_VOL, 'CATE_MS)
    resHk
  }

  // HK 최종 결과
  def getFinalResHk(resHk:DataFrame) = {
    val finalResHk = resHk.toDF("Date", "Branch", "Si/Gu", "Sido", "Gu",
      "Brand", "Market Category", "HNK Category", "Division", "Market Volume",
      "Market MS", "Category Volume", "Category MS")

    val mktTot = finalResHk.filter($"Market Category" === "Total").
      orderBy($"Sido", $"Gu", $"Brand", $"Market Category", $"HNK Category", $"Division")
    val mktDra = finalResHk.filter($"Market Category" === "Draught").
      orderBy($"Sido", $"Gu", $"Brand", $"Market Category", $"HNK Category", $"Division")
    val mktNor = finalResHk.filter($"Market Category" === "Normal").
      orderBy($"Sido", $"Gu", $"Brand", $"Market Category", $"HNK Category", $"Division")
    val mktOth = finalResHk.filter($"Market Category" === "Others").
      orderBy($"Sido", $"Gu", $"Brand", $"Market Category", $"HNK Category", $"Division")
    (mktTot, mktDra, mktNor, mktOth)
  }
}