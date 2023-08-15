/**

 */
package sool.service.aggregate.agg_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf, when}

class GetMenuAnlysResDfDk(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  // MS 계산
  def getCalcMsUdf() = {
    val calcMsUdf = udf((deno: Double, nume: Double) => {
      if (deno == 0.0) {
        if (nume == 0.0) {
          "NaN"
        } else {
          "Infinity"
        }
      } else {
        (nume / deno * 100.0).toString
      }
    })
    calcMsUdf
  }

  // 분모, 분자 데이터프레임 합친 후 지역별 MS 구하기
  def getDnAddrG(ethDt: String, martDkDenoAddrG: DataFrame, martDkNumeAddrGFinal: DataFrame) = {
    // MS 계산 udf
    val calcMsUdf = getCalcMsUdf()

    // 분모 + 분자
    val dnAddrG = martDkDenoAddrG.join(
      martDkNumeAddrGFinal, Seq("MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDR3"), "left_outer")

    val dnAddrGMs = dnAddrG.
      withColumn("YM", lit(ethDt)).na.fill(0.0).
      withColumn("QT_SOM", calcMsUdf('QT_MARKET, 'QT_MARKET_DK)).
      withColumn("AMT_SOM", calcMsUdf('AMT_MARKET, 'AMT_MARKET_DK)).
      withColumn("BOTTLE_SOM", calcMsUdf('BOTTLE_MARKET, 'BOTTLE_MARKET_DK)).
      withColumn("DEAL_SOM", calcMsUdf('DEAL_MARKET, 'DEAL_MARKET_DK))
    dnAddrGMs
  }

  // 이번 달과 저번 달과의 값 차이를 계산 하기 위해 값들이 어떤 형태인지 체크하는 udf
  def getChckGapTypeUdf() = {
    val chckGapTypeUdf = udf((curMrkt: String, bf1mMrkt: String, curMs: String, bf1mMs: String) => {
      if (curMs == null || bf1mMs == null) false
      else if (bf1mMs == "-") false
      else if (curMs == "NaN" || bf1mMs == "NaN" || curMs == "Infinity" || bf1mMs == "Infinity") false
      else if (curMs.toDouble < 0.0 || curMs.toDouble > 100.0) false
      else if (bf1mMs.toDouble < 0.0 || bf1mMs.toDouble > 100.0) false
      else if (curMrkt.toDouble < 0.0 || bf1mMrkt.toDouble < 0.0) false
      else true
    })
    chckGapTypeUdf
  }

  // 기존 결과 테이블 컬럼명 및 순서
  def getSlctCols() = {
    val slctCols = Seq(
      "MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDR3",
      "YM", "QT_MARKET", "QT_MARKET_DK", "QT_SOM", "QT_GAP",
      "AMT_MARKET", "AMT_MARKET_DK", "AMT_SOM", "AMT_GAP", "BOTTLE_MARKET",
      "BOTTLE_MARKET_DK", "BOTTLE_SOM", "BOTTLE_GAP", "DEAL_MARKET", "DEAL_MARKET_DK",
      "DEAL_SOM", "DEAL_GAP"
    ).map(col)
    slctCols
  }

  // 이번 달 분석 결과
  def getMenuAnlysResDf(dnAddrGMs: DataFrame, menuAnlysResDfBf1m: DataFrame) = {
    // 최종 분석 결과용 select
    val menuAnlysResDfBf1mSlct = menuAnlysResDfBf1m.select(
      'MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3,
      'QT_MARKET.as("QT_MARKET_EX"), 'QT_SOM.as("QT_SOM_EX"),
      'AMT_MARKET.as("AMT_MARKET_EX"), 'AMT_SOM.as("AMT_SOM_EX"),
      'BOTTLE_MARKET.as("BOTTLE_MARKET_EX"), 'BOTTLE_SOM.as("BOTTLE_SOM_EX"),
      'DEAL_MARKET.as("DEAL_MARKET_EX") , 'DEAL_SOM.as("DEAL_SOM_EX")
    )

    // 이번 달 값과 저번 달 값 차이를 계산 하기 위해 값들이 어떤 형태인지 체크하는 udf
    val chckGapTypeUdf = getChckGapTypeUdf()
    val chckGapTypeMap = Map(
      "QT_GAP" -> chckGapTypeUdf('QT_MARKET, 'QT_MARKET_EX, 'QT_SOM, 'QT_SOM_EX),
      "AMT_GAP" -> chckGapTypeUdf('AMT_MARKET, 'AMT_MARKET_EX, 'AMT_SOM, 'AMT_SOM_EX),
      "BOTTLE_GAP" -> chckGapTypeUdf('BOTTLE_MARKET, 'BOTTLE_MARKET_EX, 'BOTTLE_SOM, 'BOTTLE_SOM_EX),
      "DEAL_GAP" -> chckGapTypeUdf('DEAL_MARKET, 'DEAL_MARKET_EX, 'DEAL_SOM, 'DEAL_SOM_EX)
    )

    // 기존 결과 테이블 컬럼명 및 순서
    val slctCols = getSlctCols()

    // 이번 달 분모 + 분자 데이터와 저번 달 결과 데이터 조인
    val dnAddrGWithBf1m = dnAddrGMs.
      join(menuAnlysResDfBf1mSlct, Seq("MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDR3"), "full_outer")

    // 이번 달 값과 저번 달 값 차이 계산
    val dnAddrGWithBf1mAddCols = dnAddrGWithBf1m.
      withColumn("QT_GAP", when(chckGapTypeMap("QT_GAP"), 'QT_SOM - 'QT_SOM_EX).otherwise("-")).
      withColumn("AMT_GAP", when(chckGapTypeMap("AMT_GAP"), 'AMT_SOM - 'AMT_SOM_EX).otherwise("-")).
      withColumn("BOTTLE_GAP", when(chckGapTypeMap("BOTTLE_GAP"), 'BOTTLE_SOM - 'BOTTLE_SOM_EX).otherwise("-")).
      withColumn("DEAL_GAP", when(chckGapTypeMap("DEAL_GAP"),'DEAL_SOM - 'DEAL_SOM_EX).otherwise("-"))

    // 최종 결과
    val menuAnlysResDf = dnAddrGWithBf1mAddCols.filter('QT_MARKET.isNotNull).select(slctCols: _*)
    menuAnlysResDf
  }

  // 메인
  def getMenuAnlysResDfDk(ethDt: String,
                          martDkDenoAddrG: DataFrame,
                          martDkNumeAddrG: DataFrame,
                          menuAnlysResDfBf1m: DataFrame) = {
    // 분모 + 분자 데이터의 지역별 MS
    val dnAddrGMs = getDnAddrG(ethDt, martDkDenoAddrG, martDkNumeAddrG)

    // 최종 결과: 메뉴별, 지역별 MS 및 저번 달 MS 값과 차이 계산
    val menuAnlysResDf = getMenuAnlysResDf(dnAddrGMs, menuAnlysResDfBf1m)
    menuAnlysResDf
  }

  /* 분석 연월이 첫달일 경우 GAP컬럼 임의 채우기 */
  def getMenuAnlysResDfFirst(dnAddrGMs: DataFrame) = {
    // 최종 분석 결과용 select
    // 기존 결과 테이블 컬럼명 및 순서
    val slctCols = getSlctCols()

    // 이번 달 값과 저번 달 값 차이 계산
    val dnAddrGMsCols = dnAddrGMs.
      withColumn("QT_GAP", lit("-")).
      withColumn("AMT_GAP", lit("-")).
      withColumn("BOTTLE_GAP", lit("-")).
      withColumn("DEAL_GAP", lit("-"))

    // 최종 결과
    val menuAnlysResDf = dnAddrGMsCols.filter('QT_MARKET.isNotNull).select(slctCols: _*)
    menuAnlysResDf
  }

  /* 분석 연월이 첫달일 경우 GAP컬럼 임의 채우기 */
  def getMenuAnlysResDfDkFist(ethDt: String,
                              martDkDenoAddrG: DataFrame,
                              martDkNumeAddrG: DataFrame ) ={
    // 분모 + 분자 데이터의 지역별 MS
    val dnAddrGMs = getDnAddrG(ethDt, martDkDenoAddrG, martDkNumeAddrG)

    // 최종 결과: GAP 컬럼 추가
    val menuAnlysResDf = getMenuAnlysResDfFirst(dnAddrGMs)
    menuAnlysResDf
  }
}