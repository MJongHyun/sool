package sool.verification.verfiy

import org.apache.spark.sql.DataFrame
import sool.common.function.{FileFunc, GetTime}
import sool.common.path.FilePath

class VerifyHK(spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val fileFuncCls = new FileFunc(spark)

  def verifyNum(df: DataFrame)={
    val checkBoolean = df.filter($"Market Volume".isNull || $"Category Volume".isNull  || $"Market MS".isNull  || $"Category MS".isNull).count == 0 // 얘가 false면 문제
    if (checkBoolean == false) {
      println(s"${getTimeCls.getTimeStamp()}, " +
        s"resultHK.parquet 데이터 값에 문제가 있으니 확인하십시오.")
    }
    checkBoolean
  }

  def runVerifyHK(ethDt: String, flag: String){
    val filePathCls = new FilePath(ethDt, flag)
    val resultHK = fileFuncCls.rParquet(filePathCls.hkResPath).toDF("Date", "Branch", "Si/Gu", "Sido", "Gu", "Brand", "Market Category", "HNK Category", "Division", "Market Volume", "Market MS", "Category Volume", "Category MS")

    if (verifyNum(resultHK) == false) return

    // 분석 2차 데이터 마트, HK 데이터 마트 불러오기
    val martHK = fileFuncCls.rParquet(filePathCls.hkwMartPath).na.fill("Total")
    // 분모가 0 이하인 값들 추출
    val targetResult = resultHK.filter($"Market Volume" <= 0 || $"Category Volume" <= 0)

    // 결과에서 필요한 값만 불러오기
    val targetRes = targetResult.
      filter($"Division" === "Amount").
      select("Sido", "Gu", "Market Category", "HNK Category", "Market Volume", "Category Volume").distinct().
      toDF("ADDR1", "ADDR2", "MKT_CTGY_NM", "HK_CTGY_NM", "Market Volume", "Category Volume")
    // HK 마트데이터와 조인하여 사업자 거래 정보 확인 및 매칭이전값 결과값 추출
    val totalRes = martHK.join(targetRes, Seq("ADDR1", "ADDR2", "MKT_CTGY_NM", "HK_CTGY_NM")).
      select("BYR_RGNO", "BYR_NM", "BYR_ADDR", "ADDR1", "ADDR2", "NAME", "ITEM_SZ", "ITEM", "SUP_AMT", "QT", "ITEM_QT", "MKT_CTGY_NM", "HK_CTGY_NM", "Market Volume", "Category Volume").
      orderBy("ADDR1", "ADDR2", "MKT_CTGY_NM", "HK_CTGY_NM")

    val reportRes = totalRes.filter($"SUP_AMT" < 0).select("BYR_RGNO", "BYR_NM", "ADDR1", "ADDR2", "ITEM", "ITEM_QT", "QT", "SUP_AMT").toDF("사업자", "사업자명", "시도", "시군구", "아이템명", "수량", "매입량", "매입액").distinct()

    fileFuncCls.multiSheetXlsx(totalRes, "검증", filePathCls.hkVerifyPath)
    fileFuncCls.multiSheetXlsx(reportRes, "보고", filePathCls.hkVerifyPath)
  }
}
