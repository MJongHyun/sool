/**

 */
package sool.service.aggregate.agg_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, max, struct, to_json, udf}

class GetViewRes(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  // 필요 클래스 선언
  val dkYmCls = new DkYm()

  // ADDRESS 컬럼 추가하기
  def addAddressCol(totalDkb: DataFrame, totalDkw: DataFrame) = {
    val addColUdf = udf((addrLvl: String, addr1: String, addr2: String, addr3: String) => {
      if (addrLvl == "0") addr1
      else if (addrLvl == "1") addr2
      else addr3
    })

    val totalDkbAddCol = totalDkb.withColumn("ADDRESS", addColUdf('ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3)).
      drop("ADDR3")
    val totalDkwAddCol = totalDkw.withColumn("ADDRESS", addColUdf('ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3)).
      drop("ADDR3")
    (totalDkbAddCol, totalDkwAddCol)
  }

  // struct 형태 데이터프레임 추출
  def getStructDf(totalDkbAddCol: DataFrame, totalDkwAddCol: DataFrame) = {
    val slctCols = Seq(
      col("YM"),
      struct('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDRESS).as("HEAD"),
      col("DEAL_MARKET"),
      struct('QT_MARKET.as("MARKET"), 'QT_SOM.as("SOM"), 'QT_GAP.as("GAP")).as("QT"),
      struct('AMT_MARKET.as("MARKET"), 'AMT_SOM.as("SOM"), 'AMT_GAP.as("GAP")).as("AMT"),
      struct('BOTTLE_MARKET.as("MARKET"), 'BOTTLE_SOM.as("SOM"), 'BOTTLE_GAP.as("GAP")).as("BOTTLE"),
      struct('DEAL_MARKET.as("MARKET"), 'DEAL_SOM.as("SOM"), 'DEAL_GAP.as("GAP")).as("DEALER")
    )

    val totalDkbStruct = totalDkbAddCol.na.fill("").select(slctCols:_*)
    val totalDkwStruct = totalDkwAddCol.na.fill("").select(slctCols:_*)
    (totalDkbStruct, totalDkwStruct)
  }

  // AMT, DEALER, QT 관련 피봇 및 json 형태 데이터프레임 생성
  def getWebView(totalDkStruct: DataFrame, dkY: String) = {
    // QT 피봇
    val webViewQtP = totalDkStruct.select('YM, 'HEAD, 'QT).groupBy('HEAD).
      pivot("YM").agg(max('QT).as("QT"))
    // AMT 피봇
    val webViewAmtP = totalDkStruct.select('YM, 'HEAD, 'AMT).groupBy('HEAD).
      pivot("YM").agg(max('AMT).as("AMT"))
    // DEALER 피봇
    val webViewDealerP = totalDkStruct.select('YM, 'HEAD, 'DEALER).groupBy('HEAD).
      pivot("YM").agg(max('DEALER).as("DEALER"))

    // HEAD 관련 컬럼들
    val headCols = Seq("HEAD.MENU_ID", "HEAD.ADDR_LVL", "HEAD.ADDR1", "HEAD.ADDR2", "HEAD.ADDRESS").map(col)
    // FISCAL_YEAR 컬럼 추가
    val fiscalYearCol = Seq(lit(dkY).as("FISCAL_YEAR"))
    // 공통 컬럼
    val commonCols = webViewAmtP.columns.intersect(webViewDealerP.columns).intersect(webViewQtP.columns)
    // DK 회계연도 컬럼, ex: F2101 ...
    val dkYmCols = commonCols.filter(_.startsWith("F")).map(col)
    // json 형태 컬럼
    val jsonCol = Seq(to_json(struct(dkYmCols:_*)))

    // 최종 선택 컬럼 및 컬럼 순서
    val qtSlctCols = headCols ++ fiscalYearCol ++ jsonCol.map(_.as("DATA_QT"))
    val amtSlctCols = headCols ++ fiscalYearCol ++ jsonCol.map(_.as("DATA_AMT"))
    val dealerSlctCols = headCols ++ fiscalYearCol ++ jsonCol.map(_.as("DATA_DEALER"))

    val webViewQt = webViewQtP.select(qtSlctCols:_*)
    val webViewAmt = webViewAmtP.select(amtSlctCols:_*)
    val webViewDealer = webViewDealerP.select(dealerSlctCols:_*)

    val joinCols = Seq("MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDRESS", "FISCAL_YEAR")
    val webView = webViewQt.join(webViewAmt, joinCols).join(webViewDealer, joinCols)
    webView
  }

  // 엑셀용 데이터프레임 생성
  def getExcelView(dkYm: String, totalDkbAddCol: DataFrame, totalDkwAddCol: DataFrame) = {
    // 컬럼 순서
    val rootCols = Seq(
      "YM", "MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDRESS",
      "QT_MARKET", "QT_SOM", "QT_GAP", "AMT_MARKET", "AMT_SOM", "AMT_GAP",
      "BOTTLE_MARKET", "BOTTLE_SOM", "BOTTLE_GAP"
    ).map(col)

    // 이름 바꿔야 하는 컬럼들, 엑셀 결과에는 DEAL 이 DEALER 로 이름이 바뀌어서 제공되는데 히스토리를 모르겠다. 일단 기존 이름대로 한다.
    val renameCols = Seq(
      'DEAL_MARKET.as("DEALER_MARKET"),
      'DEAL_SOM.as("DEALER_SOM"),
      'DEAL_GAP.as("DEALER_GAP")
    )

    // 최종 컬럼 및 컬럼 순서
    val slctCols = rootCols ++ renameCols

    val dkbExcel = totalDkbAddCol.select(slctCols:_*)
    val dkwExcel = totalDkwAddCol.select(slctCols:_*)
    (dkbExcel, dkwExcel)
  }

  def getViewResNew(ethDt:String, totalDkb: DataFrame, totalDkw: DataFrame) = {
    // 현재 집계연월을 DK 회계연월로 구하기
    val (dkYm, dkY, _) = dkYmCls(ethDt)

    // ADDRESS 컬럼 추가
    val (totalDkbAddCol, totalDkwAddCol) = addAddressCol(totalDkb, totalDkw)

    // 엑셀 용 데이터프레임 생성
    val (dkbExcel, dkwExcel) = getExcelView(dkYm, totalDkbAddCol, totalDkwAddCol)
    (dkbExcel, dkwExcel)
  }
  def getViewRes(ethDt:String, totalDkb: DataFrame, totalDkw: DataFrame) = {
    // 현재 집계연월을 DK 회계연월로 구하기
    val (dkYm, dkY, _) = dkYmCls(ethDt)

    // ADDRESS 컬럼 추가
    val (totalDkbAddCol, totalDkwAddCol) = addAddressCol(totalDkb, totalDkw)

    // struct 형태 데이터프레임 추출
    val (totalDkbStruct, totalDkwStruct) = getStructDf(totalDkbAddCol, totalDkwAddCol)

    // WEB 용 데이터프레임 생성
    val dkbWeb = getWebView(totalDkbStruct, dkY)
    val dkwWeb = getWebView(totalDkwStruct, dkY)

    // 엑셀 용 데이터프레임 생성
    val (dkbExcel, dkwExcel) = getExcelView(dkYm, totalDkbAddCol, totalDkwAddCol)
    (dkbWeb, dkwWeb, dkbExcel, dkwExcel)
  }
}