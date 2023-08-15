/**

 */
package sool.service.aggregate.agg_hj

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{bround, col, countDistinct, lit, split, sum, when}
import org.apache.spark.sql.types.DecimalType

class GetAnvrDfsHj(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 분모, 주소별로 그룹화 Aggregation 함수
  def denoDfGAgg(denoDfG:RelationalGroupedDataset) = denoDfG.agg(
    countDistinct('COM_RGNO).as("TOTAL_CNT"),
    sum("SUM_QT").as("TOTAL_QT"),
    sum("SUM_SUP_AMT").as("TOTAL_SUP_AMT")
  )

  // 분모, 주소별로 그룹화
  def getDenoLvlG(denoDf:DataFrame) = {
    val denoLvl0 = denoDfGAgg(denoDf.groupBy('DENOM_CD, 'LEVEL0))
    val denoLvl1 = denoDfGAgg(denoDf.groupBy('DENOM_CD, 'LEVEL1))
    val denoLvl2 = denoDfGAgg(denoDf.
      filter('LEVEL1 =!= "세종특별자치시").
      groupBy('DENOM_CD, 'LEVEL1, 'LEVEL2)
    )
    val denoLvl2Si = denoDfGAgg(denoDf.
      filter('LEVEL1 =!= "세종특별자치시").
      filter('LEVEL2.contains(" ")).
      withColumn("LEVEL2", split('LEVEL2, " ").getItem(0)). // OO시 만 추출
      groupBy('DENOM_CD, 'LEVEL1, 'LEVEL2)
    )
    val denoLvl3 = denoDfGAgg(denoDf.groupBy('DENOM_CD, 'LEVEL1, 'LEVEL2, 'LEVEL3))
    val denoLvl4 = denoDfGAgg(denoDf.groupBy('DENOM_CD, 'LEVEL1, 'LEVEL2, 'LEVEL3, 'LEVEL4))
    val denoLvl5 = denoDfGAgg(denoDf.groupBy('DENOM_CD, 'LEVEL1, 'LEVEL2, 'POST_CD))
    val denoLvl6 = denoDfGAgg(denoDf.groupBy('DENOM_CD, 'LEVEL1, 'LEVEL2, 'LEVEL3, 'LEVEL4, 'POST_CD))
    (denoLvl0, denoLvl1, denoLvl2, denoLvl2Si, denoLvl3, denoLvl4, denoLvl5, denoLvl6)
  }

  // 분모, 주소별로 그룹화 Aggregation 함수
  def numeDfGAgg(numeDfG:RelationalGroupedDataset) = numeDfG.agg(
    countDistinct('COM_RGNO).as("CNT"),
    sum("SUM_QT").as("QT"),
    sum("SUM_SUP_AMT").as("SUP_AMT")
  )

  // 분자, 주소별로 그룹화
  def getNumeLvlG(numeDf:DataFrame) = {
    val numeLvl0 = numeDfGAgg(numeDf.groupBy('NUMER_CD, 'LEVEL0))
    val numeLvl1 = numeDfGAgg(numeDf.groupBy('NUMER_CD, 'LEVEL1))
    val numeLvl2 = numeDfGAgg(numeDf.
      filter('LEVEL1 =!= "세종특별자치시").
      groupBy('NUMER_CD, 'LEVEL1, 'LEVEL2)
    )
    val numeLvl2Si = numeDfGAgg(numeDf.
      filter('LEVEL1 =!= "세종특별자치시").
      filter('LEVEL2.contains(" ")).
      withColumn("LEVEL2", split('LEVEL2, " ").getItem(0))
      groupBy('NUMER_CD, 'LEVEL1, 'LEVEL2)
    )
    val numeLvl3 = numeDfGAgg(numeDf.groupBy('NUMER_CD, 'LEVEL1, 'LEVEL2, 'LEVEL3))
    val numeLvl4 = numeDfGAgg(numeDf.groupBy('NUMER_CD, 'LEVEL1, 'LEVEL2, 'LEVEL3, 'LEVEL4))
    val numeLvl5 = numeDfGAgg(numeDf.groupBy('NUMER_CD, 'LEVEL1, 'LEVEL2, 'POST_CD))
    val numeLvl6 = numeDfGAgg(numeDf.groupBy('NUMER_CD, 'LEVEL1, 'LEVEL2, 'LEVEL3, 'LEVEL4, 'POST_CD))
    (numeLvl0, numeLvl1, numeLvl2, numeLvl2Si, numeLvl3, numeLvl4, numeLvl5, numeLvl6)
  }

  // 레벨별 컬럼들
  def getLvlColsMap() = {
    val lvlColsMap = Map(
      "lvl0" -> Seq("NUMER_CD", "DENOM_CD", "LEVEL1"),
      "lvl1" -> Seq("NUMER_CD", "DENOM_CD", "LEVEL1"),
      "lvl2" -> Seq("NUMER_CD", "DENOM_CD", "LEVEL1", "LEVEL2"),
      "lvl3" -> Seq("NUMER_CD", "DENOM_CD", "LEVEL1", "LEVEL2", "LEVEL3"),
      "lvl4" -> Seq("NUMER_CD", "DENOM_CD", "LEVEL1", "LEVEL2", "LEVEL3", "LEVEL4"),
      "lvl5" -> Seq("NUMER_CD", "DENOM_CD", "LEVEL1", "LEVEL2", "POST_CD"),
      "lvl6" -> Seq("NUMER_CD", "DENOM_CD", "LEVEL1", "LEVEL2", "POST_CD", "LEVEL3", "LEVEL4")
    )
    lvlColsMap
  }

  // 결과 테이블에 소수점 처리
  // Q. bround(오사오입)로 처리한 이유를 모르겠음. 일단 기존 코드 그대로 사용.
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

  // 결과별로 집계연월 및 ANVR 코드 컬럼 추가
  def addYmAnvrSetCdCols(resPre: DataFrame, ethDt: String, anvrCd: String): DataFrame = {
    val res = resPre.
      withColumn("YM", lit(ethDt)).
      withColumn("ANVR_SET_CD", lit(anvrCd))
    res
  }

  // 값들을 Decimal 타입으로 변경 및 일부 값 마스킹 처리
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

  // Lvl0 결과
  def getResLvl0(hjMnIntrSlct:DataFrame,
                 denoLvl0:DataFrame,
                 numeLvl0:DataFrame,
                 lvlCols:Seq[String]) = {
    // Lvl0
    val resLvl0Pre = hjMnIntrSlct.
      join(denoLvl0, Seq("DENOM_CD")).
      join(numeLvl0, Seq("NUMER_CD", "LEVEL0"), "left_outer").
      withColumnRenamed("LEVEL0", "LEVEL1").
      na.fill(0.0)
    val resLvl0 = doBround(resLvl0Pre, lvlCols)
    resLvl0
  }

  // Lvl1 결과
  def getResLvl1(hjMnIntrSlct:DataFrame,
                 denoLvl1:DataFrame,
                 numeLvl1:DataFrame,
                 lvlCols:Seq[String],
                 resLvl0:DataFrame,
                 ethDt:String) = {
    val resLvl1Pre = hjMnIntrSlct.
      join(denoLvl1, Seq("DENOM_CD")).
      join(numeLvl1, Seq("NUMER_CD", "LEVEL1"), "left_outer").
      na.fill(0.0)

    val resLvl1Bround = doBround(resLvl1Pre, lvlCols)
    val resLvl1UnionLvl0 = resLvl1Bround.union(resLvl0)
    val resLvl1AddCols = addYmAnvrSetCdCols(resLvl1UnionLvl0, ethDt, "ANVR01")
    val resLvl1 = convertDecimalAndMasking(resLvl1AddCols)
    resLvl1
  }

  // Lvl2Si 결과
  def getResLvl2Si(hjMnIntrSlct:DataFrame,
                   denoLvl2Si:DataFrame,
                   numeLvl2Si:DataFrame,
                   lvlCols:Seq[String]) = {
    val resLvl2SiPre = hjMnIntrSlct.
      join(denoLvl2Si, Seq("DENOM_CD")).
      join(numeLvl2Si, Seq("NUMER_CD", "LEVEL1", "LEVEL2"), "left_outer").
      na.fill(0.0)
    val resLvl2Si = doBround(resLvl2SiPre, lvlCols)
    resLvl2Si
  }

  // Lvl2 결과
  def getResLvl2(hjMnIntrSlct:DataFrame,
                 denoLvl2:DataFrame,
                 numeLvl2:DataFrame,
                 lvlCols:Seq[String],
                 resLvl2Si:DataFrame,
                 ethDt:String) = {
    val resLvl2Pre = hjMnIntrSlct.
      join(denoLvl2, Seq("DENOM_CD")).
      join(numeLvl2, Seq("NUMER_CD", "LEVEL1", "LEVEL2"), "left_outer").
      na.fill(0.0)
    val resLvl2Bround = doBround(resLvl2Pre, lvlCols)
    val resLvl2UnionLvl2Si = resLvl2Bround.union(resLvl2Si)
    val resLvl2AddCols = addYmAnvrSetCdCols(resLvl2UnionLvl2Si, ethDt, "ANVR02")
    val resLvl2 = convertDecimalAndMasking(resLvl2AddCols)
    resLvl2
  }

  // Lvl3 결과
  def getResLvl3(hjMnIntrSlct:DataFrame,
                 denoLvl3:DataFrame,
                 numeLvl3:DataFrame,
                 lvlCols:Seq[String],
                 ethDt:String) = {
    val resLvl3Pre = hjMnIntrSlct.
      join(denoLvl3, Seq("DENOM_CD")).
      join(numeLvl3, Seq("NUMER_CD", "LEVEL1", "LEVEL2", "LEVEL3"), "left_outer").
      na.fill(0.0)
    val resLvl3Bround = doBround(resLvl3Pre, lvlCols)
    val resLvl3AddCols = addYmAnvrSetCdCols(resLvl3Bround, ethDt, "ANVR03")
    val resLvl3 = convertDecimalAndMasking(resLvl3AddCols)
    resLvl3
  }

  // Lvl4 결과
  def getResLvl4(hjMnIntrSlct:DataFrame,
                 denoLvl4:DataFrame,
                 numeLvl4:DataFrame,
                 lvlCols:Seq[String],
                 ethDt:String) = {
    val resLvl4Pre = hjMnIntrSlct.
      join(denoLvl4, Seq("DENOM_CD")).
      join(numeLvl4, Seq("NUMER_CD", "LEVEL1", "LEVEL2", "LEVEL3", "LEVEL4"), "left_outer").
      na.fill(0.0)
    val resLvl4Bround = doBround(resLvl4Pre, lvlCols)
    val resLvl4AddCols = addYmAnvrSetCdCols(resLvl4Bround, ethDt, "ANVR04")
    val resLvl4 = convertDecimalAndMasking(resLvl4AddCols)
    resLvl4
  }

  // Lvl5 결과
  def getResLvl5(hjMnIntrSlct:DataFrame,
                 denoLvl5:DataFrame,
                 numeLvl5:DataFrame,
                 lvlCols:Seq[String],
                 ethDt:String) = {
    val resLvl5Pre = hjMnIntrSlct.
      join(denoLvl5, Seq("DENOM_CD")).
      join(numeLvl5, Seq("NUMER_CD", "LEVEL1", "LEVEL2", "POST_CD"), "left_outer").
      na.fill(0.0)
    val resLvl5Bround = doBround(resLvl5Pre, lvlCols)
    val resLvl5AddCols = addYmAnvrSetCdCols(resLvl5Bround, ethDt, "ANVR05")
    val resLvl5 = convertDecimalAndMasking(resLvl5AddCols)
    resLvl5
  }

  // Lvl6 결과
  def getResLvl6(hjMnIntrSlct:DataFrame,
                 denoLvl6:DataFrame,
                 numeLvl6:DataFrame,
                 lvlCols:Seq[String],
                 ethDt:String) = {
    val resLvl6Pre = hjMnIntrSlct.
      join(denoLvl6, Seq("DENOM_CD")).
      join(numeLvl6, Seq("NUMER_CD", "LEVEL1", "LEVEL2", "POST_CD", "LEVEL3", "LEVEL4"), "left_outer").
      na.fill(0.0)
    val resLvl6Bround = doBround(resLvl6Pre, lvlCols)
    val resLvl6AddCols = addYmAnvrSetCdCols(resLvl6Bround, ethDt, "ANVR06")
    val resLvl6 = convertDecimalAndMasking(resLvl6AddCols)
    resLvl6
  }

  // 맥주, 소주별 ANVR01 ~ ANVR06 생성
  def getResLvlHj(denoDf:DataFrame, numeDf:DataFrame, hjMnIntr:DataFrame, ethDt:String) = {
    // 메뉴 개요 테이블 일부 수정 및 select
    val hjMnIntrSlct = hjMnIntr.select('FLTR_ID1.as("DENOM_CD"), 'FLTR_ID2.as("NUMER_CD"))

    // 주소별 그룹화
    val (denoLvl0, denoLvl1, denoLvl2, denoLvl2Si, denoLvl3, denoLvl4, denoLvl5, denoLvl6) = getDenoLvlG(denoDf) // 분모
    val (numeLvl0, numeLvl1, numeLvl2, numeLvl2Si, numeLvl3, numeLvl4, numeLvl5, numeLvl6) = getNumeLvlG(numeDf) // 분자

    // 레벨별 컬럼들
    val lvlColsMap = getLvlColsMap()

    // 레벨별 결과 생성
    val resLvl0 = getResLvl0(hjMnIntrSlct, denoLvl0, numeLvl0, lvlColsMap("lvl0"))  // resLvl1 에 병합됨
    val resLvl1 = getResLvl1(hjMnIntrSlct, denoLvl1, numeLvl1, lvlColsMap("lvl1"), resLvl0, ethDt)
    val resLvl2Si = getResLvl2Si(hjMnIntrSlct, denoLvl2Si, numeLvl2Si, lvlColsMap("lvl2"))  // resLvl2 에 병합됨
    val resLvl2 = getResLvl2(hjMnIntrSlct, denoLvl2, numeLvl2, lvlColsMap("lvl2"), resLvl2Si, ethDt)
    val resLvl3 = getResLvl3(hjMnIntrSlct, denoLvl3, numeLvl3, lvlColsMap("lvl3"), ethDt)
    val resLvl4 = getResLvl4(hjMnIntrSlct, denoLvl4, numeLvl4, lvlColsMap("lvl4"), ethDt)
    val resLvl5 = getResLvl5(hjMnIntrSlct, denoLvl5, numeLvl5, lvlColsMap("lvl5"), ethDt)
    val resLvl6 = getResLvl6(hjMnIntrSlct, denoLvl6, numeLvl6, lvlColsMap("lvl6"), ethDt)
    (resLvl1, resLvl2, resLvl3, resLvl4, resLvl5, resLvl6)
  }

  // 메인
  def getAnvrDfsHj(martHjbLvlDeno: DataFrame,
                   martHjsLvlDeno: DataFrame,
                   martHjbLvlNume: DataFrame,
                   martHjsLvlNume: DataFrame,
                   hjMnIntr: DataFrame,
                   ethDt: String) = {
    // 맥주, 소주별 ANVR01 ~ ANVR06 생성
    val (hjbResLvl1, hjbResLvl2, hjbResLvl3, hjbResLvl4, hjbResLvl5, hjbResLvl6) =
      getResLvlHj(martHjbLvlDeno, martHjbLvlNume, hjMnIntr, ethDt)
    val (hjsResLvl1, hjsResLvl2, hjsResLvl3, hjsResLvl4, hjsResLvl5, hjsResLvl6) =
      getResLvlHj(martHjsLvlDeno, martHjsLvlNume, hjMnIntr, ethDt)

    // 전체(맥주 + 소주) ANVR01 ~ ANVR06 생성
    val anvr01 = hjbResLvl1.union(hjsResLvl1)
    val anvr02 = hjbResLvl2.union(hjsResLvl2)
    val anvr03 = hjbResLvl3.union(hjsResLvl3)
    val anvr04 = hjbResLvl4.union(hjsResLvl4)
    val anvr05 = hjbResLvl5.union(hjsResLvl5)
    val anvr06 = hjbResLvl6.union(hjsResLvl6)
    (anvr01, anvr02, anvr03, anvr04, anvr05, anvr06)
  }
}
