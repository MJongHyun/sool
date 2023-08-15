/**

 */
package sool.service.aggregate.agg_hj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct, lit, sum, udf, when}

class GetAnvr13DfsHj(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // comMain 과 lv2w 조인
  def getComMainLv2w(comMain :DataFrame, lv2w :DataFrame) = {
    val comMainPstpr = comMain.
      filter('ADDR1.isNotNull).
      select("COM_RGNO", "ADDR1", "ADDR2").
      na.fill("", Seq("ADDR2")).
      withColumnRenamed("ADDR1", "LEVEL1").
      withColumnRenamed("ADDR2", "LEVEL2")

    val comMainLv2w = comMainPstpr.join(lv2w, Seq("LEVEL1", "LEVEL2")).distinct
    comMainLv2w
  }

  // 공급업체 기준 마트 생성
  // 주소 정제 안한 HJ 마트 데이터에 comMain, lv2w 을 붙여서 공급업체별 주소, 아이템, 각 집계값 구하기
  def getHjSupMart(comMainLv2w: DataFrame,
                   martHjbNonAddr: DataFrame,
                   martHjsNonAddr: DataFrame,
                   hjbItem: DataFrame,
                   hjsItem: DataFrame) = {
    val martHjbNonAddrPstpr = martHjbNonAddr.
      withColumnRenamed("COM_RGNO", "BYR_RGNO").
      withColumnRenamed("SUP_RGNO", "COM_RGNO")
    val martHjsNonAddrPstpr = martHjsNonAddr.
      withColumnRenamed("COM_RGNO", "BYR_RGNO").
      withColumnRenamed("SUP_RGNO", "COM_RGNO")

    val hjbComMainLv2w = martHjbNonAddrPstpr.join(comMainLv2w, Seq("COM_RGNO"))
    val hjsComMainLv2w = martHjsNonAddrPstpr.join(comMainLv2w, Seq("COM_RGNO"))

    val hjbComLv2wItemAgg = hjbComMainLv2w.
      groupBy('COM_RGNO, 'LEVEL2W, 'ITEM).
      agg(sum('ITEM_QT).as("SUM_ITEM_QT"),
          sum('QT).as("SUM_QT"),
          sum('SUP_AMT).as("SUM_SUP_AMT"))
    val hjsComLv2wItemAgg = hjsComMainLv2w.
      groupBy('COM_RGNO, 'LEVEL2W, 'ITEM).
      agg(sum('ITEM_QT).as("SUM_ITEM_QT"),
        sum('QT).as("SUM_QT"),
        sum('SUP_AMT).as("SUM_SUP_AMT"))

    val hjbSupMart = hjbComLv2wItemAgg.join(hjbItem, hjbComLv2wItemAgg("ITEM") === hjbItem("BR_NM"))
    val hjsSupMart = hjsComLv2wItemAgg.join(hjsItem, hjsComLv2wItemAgg("ITEM") === hjsItem("SJ_NM"))

    (hjbSupMart, hjsSupMart)
  }

  // 공급업체 기준 HJ 마트 필터 쿼리 생성
  def getHjSupMartFltrQry(supCdDimension: DataFrame, hjFltrQry: DataFrame) = {
    val supCdDimensionDenomCd = supCdDimension.select('DENOM_CD.as("FLTR_ID"))
    val supCdDimensionNumerCd = supCdDimension.select('NUMER_CD.as("FLTR_ID"))
    val supCdDimensionCd = supCdDimensionDenomCd.union(supCdDimensionNumerCd).distinct

    val hjFltrQrySlct = hjFltrQry.select('FLTR_ID, 'QRY_C, 'ITEM_TBL_CD)
    val hjSupMartFltrQry = hjFltrQrySlct.join(supCdDimensionCd, Seq("FLTR_ID"))
    hjSupMartFltrQry
  }

  // supCdDimension 에 쿼리, 아이템 테이블 코드 붙이기
  def getSupCdDimensionQry(supCdDimension:DataFrame, hjSupMartFltrQry:DataFrame) = {
    val supCdDimensionSlct = supCdDimension.select("DENOM_CD", "NUMER_CD")
    val supCdDimensionDeno = supCdDimensionSlct.
      join(hjSupMartFltrQry, supCdDimensionSlct("DENOM_CD") === hjSupMartFltrQry("FLTR_ID")).
      withColumnRenamed("QRY_C", "DENOM_QRY").
      withColumnRenamed("ITEM_TBL_CD", "D_ITEM_TBL_CD").
      drop("FLTR_ID")
    val supCdDimensionDenoNume = supCdDimensionDeno.
      join(hjSupMartFltrQry, supCdDimensionDeno("NUMER_CD") === hjSupMartFltrQry("FLTR_ID")).
      withColumnRenamed("QRY_C", "NUMER_QRY").
      withColumnRenamed("ITEM_TBL_CD", "N_ITEM_TBL_CD").
      drop("FLTR_ID")
    val supCdDimensionQry = supCdDimensionDenoNume.select(
      "DENOM_CD", "DENOM_QRY", "D_ITEM_TBL_CD",
      "NUMER_CD", "NUMER_QRY", "N_ITEM_TBL_CD"
    )
    supCdDimensionQry
  }

  // supCdDimension 에 따른 분모, 분자 데이터 결과
  def getHjSupDenoNume(supCdDimensionQry: DataFrame, hjbSupMart: DataFrame, hjsSupMart: DataFrame) = {
    val hjSupDenoNume = supCdDimensionQry.as[(String,String,String,String,String,String)].collect.map(i => {
      val (denomCd, denomQry, dItemTblCd, numerCd, numerQry, nItemTblCd) =
        (i._1, i._2, i._3, i._4, i._5, i._6)

      val slctCols = Seq("COM_RGNO", "BRND_NM", "SUM_ITEM_QT", "SUM_QT", "SUM_SUP_AMT", "LEVEL2W").map(col)

      // 분모
      val denoFltr = if (dItemTblCd == "HJ_BR_ITEM") {
        hjbSupMart.filter(denomQry).select(slctCols:_*)
      } else {
        hjsSupMart.filter(denomQry).select(slctCols:_*)
      }

      val denoG = denoFltr.groupBy("LEVEL2W").agg(
        sum("SUM_QT").as("QT_MARKET"),
        sum("SUM_SUP_AMT").as("AMT_MARKET"),
        countDistinct("COM_RGNO").as("COM_MARKET")
      ).withColumn("DENOM_CD", lit(denomCd))

      // 분자
      val numeFltr = if (nItemTblCd == "HJ_BR_ITEM") {
        hjbSupMart.filter(numerQry).select(slctCols:_*)
      } else {
        hjsSupMart.filter(numerQry).select(slctCols:_*)
      }

      val numeG = numeFltr.groupBy("LEVEL2W").agg(
        sum("SUM_QT").as("QT_MARKET_HITE"),
        sum("SUM_SUP_AMT").as("AMT_MARKET_HITE"),
        countDistinct("COM_RGNO").as("COM_MARKET_HITE")
      ).withColumn("NUMER_CD", lit(numerCd))

      val res = denoG.join(numeG, Seq("LEVEL2W"), "left_outer")
      res
    }).reduce(_ union _)
    hjSupDenoNume
  }

  // COM_RGNO 값 확인 컬럼 추가
  def addYnCol(hjSupDenoNume:DataFrame) = {
    val addYnColUdf = udf((comMarket: Long) => {
      val yList = List(1, 2)  // COM_RGNO 개수가 1, 2 일 경우 Y 처리한다.
      if (yList.contains(comMarket)) "Y"
      else if (comMarket == null) "N"
      else null:String
    })
    val hjSupYn = hjSupDenoNume.
      withColumn("YN", addYnColUdf('COM_MARKET))
    hjSupYn
  }

  // MS 컬럼 추가
  def addMsCol(hjSupYn:DataFrame) = {
    val addMsColUdf = udf((hite: Double, market: Double) => {
      if (market == 0.0) {
        0.0
      } else {
        (hite / market * 10000).round / 100.0
      }
    })
    val hjSupYnNaFill = hjSupYn.
      na.fill(0.0, Seq("QT_MARKET_HITE", "AMT_MARKET_HITE", "COM_MARKET_HITE"))
    val hjSupYnMs = hjSupYnNaFill.
      withColumn("QT_MS", addMsColUdf('QT_MARKET_HITE, 'QT_MARKET)).
      withColumn("AMT_MS", addMsColUdf( 'AMT_MARKET_HITE, 'AMT_MARKET)).
      withColumn("COM_MS", addMsColUdf( 'COM_MARKET_HITE, 'COM_MARKET))
    hjSupYnMs
  }

  // 스케일 변환
  def scaleConversion(hjSupYnMs:DataFrame) = {
    val sclCnvUdf = udf((colVal: Double, scaleVal: Double) => {
      (colVal / scaleVal * 100).round / 100.0
    })
    val hjSupYnMsSclCnv = hjSupYnMs.
      withColumn("QT_MARKET", sclCnvUdf('QT_MARKET, lit(1000.0))).
      withColumn("QT_MARKET_HITE", sclCnvUdf('QT_MARKET_HITE, lit(1000.0))).
      withColumn("AMT_MARKET", sclCnvUdf('AMT_MARKET, lit(100000.0))).
      withColumn("AMT_MARKET_HITE", sclCnvUdf('AMT_MARKET_HITE, lit(100000.0)))
    hjSupYnMsSclCnv
  }

  // 마스킹 처리
  def masking(hjSupYnMsSclCnv:DataFrame) = {
    val hjSupYnMsSclCnvMasking = hjSupYnMsSclCnv.
      withColumn("QT_MARKET", when('YN.isNotNull, 0.0).otherwise('QT_MARKET)).
      withColumn("QT_MS", when('YN.isNotNull, 0.0).otherwise('QT_MS)).
      withColumn("AMT_MARKET", when('YN.isNotNull, 0.0).otherwise('AMT_MARKET)).
      withColumn("AMT_MS", when('YN.isNotNull, 0.0).otherwise('AMT_MS)).
      withColumn("COM_MARKET", when('YN.isNotNull, 0.0).otherwise('COM_MARKET)).
      withColumn("COM_MS", when('YN.isNotNull, 0.0).otherwise('COM_MS))
    hjSupYnMsSclCnvMasking
  }

  // 최종 ANVR13 데이터프레임 생성
  def getAnvr13(ethDt: String, hjSupYnMsSclCnvMasking: DataFrame) = {
    // ANVR13 관련 컬럼 추가
    val hjSupYnMsSclCnvMaskingAddCols = hjSupYnMsSclCnvMasking.
      withColumn("YM", lit(ethDt)).
      withColumn("ANVR_SET_CD", lit("ANVR13"))

    // 최종 ANVR13 결과
    val anvr13 = hjSupYnMsSclCnvMaskingAddCols.select(
      "YM", "ANVR_SET_CD", "LEVEL2W",
      "NUMER_CD", "DENOM_CD", "QT_MARKET",
      "QT_MS", "AMT_MARKET", "AMT_MS",
      "COM_MARKET", "COM_MS", "YN"
    ).orderBy("LEVEL2W", "NUMER_CD", "DENOM_CD")
    anvr13
  }

  // 해당 클래스 메인
  def getAnvr13DfsHj(ethDt: String,
                     comMain: DataFrame,
                     lv2w: DataFrame,
                     martHjbNonAddr: DataFrame,
                     martHjsNonAddr: DataFrame,
                     hjbItem: DataFrame,
                     hjsItem: DataFrame,
                     supCdDimension: DataFrame,
                     hjFltrQry: DataFrame) = {
    // comMain 과 lv2w (ex: 진주, 울산, 부산, 대전 등) 조인
    val comMainLv2w = getComMainLv2w(comMain, lv2w)

    // 공급업체 기준 맥주, 소주 마트 생성
    val (hjbSupMart, hjsSupMart) = getHjSupMart(comMainLv2w, martHjbNonAddr, martHjsNonAddr, hjbItem, hjsItem)

    // 필터 쿼리 관련 데이터프레임
    val hjSupMartFltrQry = getHjSupMartFltrQry(supCdDimension, hjFltrQry) // supCdDimension 기준 집계 결과에 필요한 필터 추출
    val supCdDimensionQry = getSupCdDimensionQry(supCdDimension, hjSupMartFltrQry)  // supCdDimension 파일에 쿼리 붙이기

    // supCdDimension 필터 기준에 의한 공급업체 기준 맥주 + 소주 통합 분모, 분자
    val hjSupDenoNume = getHjSupDenoNume(supCdDimensionQry, hjbSupMart, hjsSupMart)

    // 컬럼 및 데이터 처리
    val hjSupYn = addYnCol(hjSupDenoNume) // COM_RGNO 값 확인 컬럼 추가
    val hjSupYnMs = addMsCol(hjSupYn) // MS 컬럼 추가
    val hjSupYnMsSclCnv = scaleConversion(hjSupYnMs)  // 스케일 변환
    val hjSupYnMsSclCnvMasking = masking(hjSupYnMsSclCnv) // 마스킹 처리

    // 최종 anvr13 결과
    val anvr13 = getAnvr13(ethDt, hjSupYnMsSclCnvMasking)
    (hjbSupMart, hjsSupMart, hjSupDenoNume, anvr13)
  }
}
