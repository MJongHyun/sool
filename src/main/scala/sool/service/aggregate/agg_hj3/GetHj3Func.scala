package sool.service.aggregate.agg_hj3

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

class GetHj3Func(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._
  // ### denoDfGAgg : 분모 매입량, 매입액, 취급업체 수 추출
  def denoDfGAgg(denoDfG:RelationalGroupedDataset) = {
    denoDfG.agg(sum("SUM_QT").as("TOTAL_QT"), sum("SUM_SUP_AMT").as("TOTAL_SUP_AMT"), countDistinct('COM_RGNO).as("TOTAL_CNT"))
  }
  // ### numeDfGAgg : 분자 매입량, 매입액, 취급업체 수 추출
  def numeDfGAgg(numeDfG:RelationalGroupedDataset) = {
    numeDfG.agg(sum("SUM_QT").as("QT"), sum("SUM_SUP_AMT").as("SUP_AMT"), countDistinct('COM_RGNO).as("CNT"))
  }
  // ### getDenoLvlG : level별 분모 매입량, 매입액 추출, 취급업체 수 추출
  def getDenoLvlG(denoDf:DataFrame) = {
    val denoLvl1 = denoDfGAgg(denoDf.groupBy('CONT, 'CONT_SIZE, 'LEVEL1))
    val denoLvl2 = denoDfGAgg(denoDf.groupBy('CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2))
    val denoLvl3 = denoDfGAgg(denoDf.groupBy('CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2, 'LEVEL3))
    val denoLvl4 = denoDfGAgg(denoDf.groupBy('CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2, 'LEVEL3, 'LEVEL4))
    val denoLvl5 = denoDfGAgg(denoDf.groupBy('CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2, 'POST_CD))
    val denoLvl6 = denoDfGAgg(denoDf.groupBy('CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2, 'POST_CD, 'LEVEL3, 'LEVEL4))
    (denoLvl1, denoLvl2, denoLvl3, denoLvl4, denoLvl5, denoLvl6)
  }
  // ### getNumeLvlG : level별 분자 매입량, 매입액, 취급업체 수 추출
  def getNumeLvlG(numeDf:DataFrame) = {
    val numeLvl1 = numeDfGAgg(numeDf.groupBy('CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE, 'LEVEL1))
    val numeLvl2 = numeDfGAgg(numeDf.groupBy('CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2))
    val numeLvl3 = numeDfGAgg(numeDf.groupBy('CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2, 'LEVEL3))
    val numeLvl4 = numeDfGAgg(numeDf.groupBy('CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2, 'LEVEL3, 'LEVEL4))
    val numeLvl5 = numeDfGAgg(numeDf.groupBy('CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2, 'POST_CD))
    val numeLvl6 = numeDfGAgg(numeDf.groupBy('CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE, 'LEVEL1, 'LEVEL2, 'POST_CD, 'LEVEL3, 'LEVEL4))
    (numeLvl1, numeLvl2, numeLvl3, numeLvl4, numeLvl5, numeLvl6)
  }
  // ### getDenoLvlG : level별 분모 매입량, 매입액, 취급업체 수 추출
  def getDenoLvlG0(denoDf:DataFrame) = {
    val denoLvl0 = denoDfGAgg(denoDf.groupBy('LEVEL0, 'CONT, 'CONT_SIZE))
    val denoLvl1 = denoDfGAgg(denoDf.groupBy('LEVEL1, 'CONT, 'CONT_SIZE))
    val denoLvl2 = denoDfGAgg(denoDf.groupBy('LEVEL1, 'LEVEL2, 'CONT, 'CONT_SIZE))
    val denoLvl3 = denoDfGAgg(denoDf.groupBy('LEVEL1, 'LEVEL2, 'LEVEL3, 'CONT, 'CONT_SIZE))
    val denoLvl4 = denoDfGAgg(denoDf.groupBy('LEVEL1, 'LEVEL2, 'LEVEL3, 'LEVEL4, 'CONT, 'CONT_SIZE))
    val denoLvl5 = denoDfGAgg(denoDf.groupBy('LEVEL1, 'LEVEL2, 'POST_CD, 'CONT, 'CONT_SIZE))
    val denoLvl6 = denoDfGAgg(denoDf.groupBy('LEVEL1, 'LEVEL2, 'POST_CD, 'LEVEL3, 'LEVEL4, 'CONT, 'CONT_SIZE))
    (denoLvl0, denoLvl1, denoLvl2, denoLvl3, denoLvl4, denoLvl5, denoLvl6)
  }
  // ### getNumeLvlG : level별 분자 매입량, 매입액, 취급업체 수 추출
  def getNumeLvlG0(numeDf:DataFrame) = {
    val numeLvl0 = numeDfGAgg(numeDf.groupBy('LEVEL0, 'CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE))
    val numeLvl1 = numeDfGAgg(numeDf.groupBy('LEVEL1, 'CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE))
    val numeLvl2 = numeDfGAgg(numeDf.groupBy('LEVEL1, 'LEVEL2, 'CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE))
    val numeLvl3 = numeDfGAgg(numeDf.groupBy('LEVEL1, 'LEVEL2, 'LEVEL3, 'CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE))
    val numeLvl4 = numeDfGAgg(numeDf.groupBy('LEVEL1, 'LEVEL2, 'LEVEL3, 'LEVEL4, 'CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE))
    val numeLvl5 = numeDfGAgg(numeDf.groupBy('LEVEL1, 'LEVEL2, 'POST_CD, 'CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE))
    val numeLvl6 = numeDfGAgg(numeDf.groupBy('LEVEL1, 'LEVEL2, 'POST_CD, 'LEVEL3, 'LEVEL4, 'CMP_CD, 'BRD_CD, 'CONT, 'CONT_SIZE))
    (numeLvl0, numeLvl1, numeLvl2, numeLvl3, numeLvl4, numeLvl5, numeLvl6)
  }
  // ### getLvlColsMap : level별 column으로 사용할 값 mapping
  def getLvlColsMap() = {
    val lvlColsMap = Map(
      "lvl0" -> Seq("YM", "LEVEL0", "ANVR_SET_CD", "CMP_CD", "BRD_CD", "CONT", "CONT_SIZE"),
      "lvl1" -> Seq("YM", "LEVEL1", "ANVR_SET_CD", "CMP_CD", "BRD_CD", "CONT", "CONT_SIZE"),
      "lvl2" -> Seq("YM", "LEVEL1", "LEVEL2", "ANVR_SET_CD", "CMP_CD", "BRD_CD", "CONT", "CONT_SIZE"),
      "lvl3" -> Seq("YM", "LEVEL1", "LEVEL2", "LEVEL3", "ANVR_SET_CD", "CMP_CD", "BRD_CD", "CONT", "CONT_SIZE"),
      "lvl4" -> Seq("YM", "LEVEL1", "LEVEL2", "LEVEL3", "LEVEL4", "ANVR_SET_CD", "CMP_CD", "BRD_CD", "CONT", "CONT_SIZE"),
      "lvl5" -> Seq("YM", "LEVEL1", "LEVEL2", "POST_CD", "ANVR_SET_CD", "CMP_CD", "BRD_CD", "CONT", "CONT_SIZE"),
      "lvl6" -> Seq("YM", "LEVEL1", "LEVEL2", "POST_CD", "LEVEL3", "LEVEL4", "ANVR_SET_CD", "CMP_CD", "BRD_CD", "CONT", "CONT_SIZE")
    )
    lvlColsMap
  }
  // ### getLvlJoinColsMap : level별 join할때 사용할 값 mapping
  def getLvlJoinColsMap() = {
    val lvlJoinColsMap = Map(
      "lvl0" -> Seq("LEVEL0", "CONT", "CONT_SIZE"),
      "lvl1" -> Seq("LEVEL1", "CONT", "CONT_SIZE"),
      "lvl2" -> Seq("LEVEL1", "LEVEL2", "CONT", "CONT_SIZE"),
      "lvl3" -> Seq("LEVEL1", "LEVEL2", "LEVEL3", "CONT", "CONT_SIZE"),
      "lvl4" -> Seq("LEVEL1", "LEVEL2", "LEVEL3", "LEVEL4", "CONT", "CONT_SIZE"),
      "lvl5" -> Seq("LEVEL1", "LEVEL2", "POST_CD", "CONT", "CONT_SIZE"),
      "lvl6" -> Seq("LEVEL1", "LEVEL2", "POST_CD", "LEVEL3", "LEVEL4", "CONT", "CONT_SIZE")
    )
    lvlJoinColsMap
  }
  // ### doBround : 단위별 환산 함수
  // QT : /1,000 (CASE), AMT : /100,000(원), MS : * 100(%)
  def doBround(resPre:DataFrame, lvlCols:Seq[String]) = {
    val slctColsStr = lvlCols ++ Seq("QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS")
    val slctCols = slctColsStr.map(col)

    val res = resPre.
      withColumn("QT_MARKET", bround('TOTAL_QT / lit(1000.0), 2)).
      withColumn("QT_MS", bround('QT / 'TOTAL_QT * 100, 2)).
      withColumn("AMT_MARKET", bround('TOTAL_SUP_AMT / lit(100000.0), 2)).
      withColumn("AMT_MS", bround('SUP_AMT / 'TOTAL_SUP_AMT * 100, 2)).
      withColumn("COM_MARKET", bround('TOTAL_CNT / lit(1.0), 2)).
      withColumn("COM_MS", bround('CNT / 'TOTAL_CNT * 100, 2)).
      select(slctCols: _*)
    res
  }
  // ### convertDecimalAndMasking : 메뉴에 따른 취급업체 수가 3개 미만인 경우 마스킹 ("-") 처리를 하는 함수
  def convertDecimalAndMasking(resPre: DataFrame): DataFrame = {
    val resPreCols = resPre.columns
    val selectCols = (resPreCols ++ Seq("YN")).map(col)

    // COM_RGNO 등장 횟수가 3 이상인 경우
    val more3Df = resPre.
      withColumn("YN",
        when('COM_MARKET.isNull, "N").
          when('COM_MARKET < 3, "Y").
          otherwise(null)).
      filter('YN.isNull).
      withColumn("QT_MARKET", 'QT_MARKET.cast(DecimalType(18,2))).
      withColumn("QT_MS", 'QT_MS.cast(DecimalType(18,2))).
      withColumn("AMT_MARKET", 'AMT_MARKET.cast(DecimalType(18,2))).
      withColumn("AMT_MS", 'AMT_MS.cast(DecimalType(18,2))).
      withColumn("COM_MARKET", 'COM_MARKET.cast(DecimalType(18,2))).
      withColumn("COM_MS", 'COM_MS.cast(DecimalType(18,2)))

    // COM_RGNO 등장 횟수가 3 미만인 경우 0.0 으로 마스킹 처리하기
    val lessThan3MaskingDf = resPre.
      withColumn("YN",
        when('COM_MARKET.isNull, "N").
          when('COM_MARKET < 3, "Y").
          otherwise(null)).
      filter('YN.isNotNull).
      withColumn("QT_MARKET", lit(0.00).cast(DecimalType(18,2))).
      withColumn("QT_MS", lit(0.00).cast(DecimalType(18,2))).
      withColumn("AMT_MARKET", lit(0.00).cast(DecimalType(18,2))).
      withColumn("AMT_MS", lit(0.00).cast(DecimalType(18,2))).
      withColumn("COM_MARKET", lit(0.00).cast(DecimalType(18,2))).
      withColumn("COM_MS", lit(0.00).cast(DecimalType(18,2)))

    // ANVR01 에는 LEVEL2 컬럼이 없어서 아래와 같이 처리함
    val more3DfNaFill = if (resPreCols.contains("LEVEL2")) {
      more3Df.
        na.fill("", Seq("LEVEL2", "YN")).
        na.fill(0.0, Seq("QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS"))
    } else {
      more3Df.
        na.fill("", Seq("YN")).
        na.fill(0.0, Seq("QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS"))
    }
    val more3DfNaFillSlct = more3DfNaFill.select(selectCols:_*)
    val lessThan3MaskingDfSlct = lessThan3MaskingDf.select(selectCols:_*)
    val res = more3DfNaFillSlct.union(lessThan3MaskingDfSlct)
    res
  }

  // ### makeResultLevelData : level별 분모/분자를 통한 결과 값 추출 - pivot값으로 변경시 함수 변경 예정
  // nume : 분자, deno: 분모, lvlCols: level별 select 컬럼,  joinCols: level별 join 컬럼, ym : 분석연월, anvr: level별 구분자
  def makeResultLevelData(nume: DataFrame, deno: DataFrame, lvlCols:Seq[String], joinCols:Seq[String], ym:String, anvr:String): DataFrame = {
    val orderByColPre = joinCols ++ Seq( "CONT", "CONT_SIZE")
    val orderByCol = orderByColPre.map(r => col(r))
    val resultDF = deno.join(nume, joinCols, "left_outer").
      na.fill(0).
      withColumn("YM", lit(ym)).
      withColumn("ANVR_SET_CD", lit(anvr))
    val levelRes = doBround(resultDF, lvlCols)
    val levelMasking = convertDecimalAndMasking(levelRes)
    val totalDF = levelMasking.
      orderBy(orderByCol:_*)
    totalDF
  }

  // ### makeFinalDF : 한 sheet에 값 저장하기 위한 데이터 추출
  // anvrDF : 결과 anvr , anvr6 : anvr6결과
  def makeFinalDF(anvrDF: DataFrame, anvr6: DataFrame): DataFrame = {
    val anvrFinalCol = anvr6.columns
    var anvrTot = anvrDF
    for(clu <- anvrFinalCol){
      if (!anvrTot.columns.contains(clu)){
        anvrTot = anvrTot.withColumn(clu, lit(""))
      }
    }
    val anvrTotal = anvrTot.select(anvrFinalCol.map(col):_*)
    anvrTotal
  }
}
