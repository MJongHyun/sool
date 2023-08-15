package sool.verification.verfiy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import sool.common.function.{FileFunc, GetTime}
import sool.common.path.FilePath

class VerifyData(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val fileFuncCls = new FileFunc(spark)


  def aggCol(df: DataFrame, colNm: String, sumAMT: String, sumQt: String, sort: String) = {
    if (sort=="BF"){
      val definedDf = df.
        agg(sum(col(sumAMT)) as "BF_TOT_AMT", sum(col(sumQt)) as "BF_TOT_QT").
        withColumn("POP", lit(colNm)).
        select("POP", "BF_TOT_AMT", "BF_TOT_QT")
      definedDf
    }
    else {
      val definedDf = df.
        agg(sum(col(sumAMT)) as "NOW_TOT_AMT", sum(col(sumQt)) as "NOW_TOT_QT").
        withColumn("POP", lit(colNm)).
        select("POP", "NOW_TOT_AMT", "NOW_TOT_QT")
      definedDf
    }
  }


  def aggColFtr(df: DataFrame, colnm: String, sumAMT: String, colFtr: String, newValue: String, sumCol: String, sort: String)= {
    if (sort=="BF"){
      val definedDf = df.
        filter(col(colnm) === colFtr).
        agg(sum(col(sumAMT)) as "BF_POP_AMT", sum(col(sumCol)) as "BF_POP_QT").
        withColumn("POP", lit(newValue)).
        select("POP", "BF_POP_AMT", "BF_POP_QT")
      definedDf
    }
    else{
      val definedDf = df.
        filter(col(colnm) === colFtr).
        agg(sum(col(sumAMT)) as "NOW_POP_AMT", sum(col(sumCol)) as "NOW_POP_QT").
        withColumn("POP", lit(newValue)).
        select("POP", "NOW_POP_AMT", "NOW_POP_QT")
      definedDf
    }
  }

  def joinRstDf(now_Tot: DataFrame, now_Pop: DataFrame, sort: String) ={
    if (sort=="BF") {
      val rst = now_Tot.
        join(now_Pop, Seq("POP")).
        withColumn("BF_AMT_MS", $"BF_POP_AMT" / $"BF_TOT_AMT").
        withColumn("BF_QT_MS", $"BF_POP_QT" / $"BF_TOT_QT")
      rst
    }
    else{
      val rst = now_Tot.
        join(now_Pop, Seq("POP")).
        withColumn("NOW_AMT_MS", $"NOW_POP_AMT" / $"NOW_TOT_AMT").
        withColumn("NOW_QT_MS", $"NOW_POP_QT" / $"NOW_TOT_QT")
      rst
    }
  }

  def mkSheet1Pre(HJ_Soju: DataFrame, HJ_Domestic_Beer: DataFrame, HJ_Oversea_Beer: DataFrame,
                  DK_Beer: DataFrame, DK_Whi: DataFrame,
                  HK_Beer: DataFrame, sort: String)={
    // HJ

    val HJ_Soju_Tot =  aggCol(HJ_Soju, "HJ모집단소주", "SUM_SUP_AMT", "SUM_QT", sort)
    val HJ_Domestic_Beer_Tot =  aggCol(HJ_Domestic_Beer,"HJ국내모집단맥주", "SUM_SUP_AMT", "SUM_QT", sort)
    val HJ_Oversea_Beer_Tot =  aggCol(HJ_Oversea_Beer,"HJ해외모집단맥주", "SUM_SUP_AMT", "SUM_QT", sort)

    // DK
    val DK_Beer_Tot =  aggCol(DK_Beer,"DK모집단수입맥주", "SUP_AMT", "QT", sort)
    val DK_Whi_Tot =  aggCol(DK_Whi,"DK모집단위스키", "SUP_AMT", "QT", sort)
    // HK
    val HK_Beer_Tot = aggCol(HK_Beer,"HK모집단수입맥주", "SUP_AMT", "QT", sort)
    // 통합
    val Tot = HJ_Soju_Tot.
      union(HJ_Domestic_Beer_Tot).
      union(HJ_Oversea_Beer_Tot).
      union(DK_Beer_Tot).
      union(DK_Whi_Tot).
      union(HK_Beer_Tot)

    // 이번달 해당 고객사의 매입금액, 매입량 추출
    // HJ
    val HJ_Soju_Pop = aggColFtr(HJ_Soju, "MFG_NM", "SUM_SUP_AMT", "HJ","HJ모집단소주", "SUM_QT", sort)
    val HJ_Domestic_Beer_Pop = aggColFtr(HJ_Domestic_Beer, "MFG_NM", "SUM_SUP_AMT", "HJ","HJ국내모집단맥주", "SUM_QT", sort)
    val HJ_Oversea_Beer_Pop = aggColFtr(HJ_Oversea_Beer, "MFG_NM","SUM_SUP_AMT",  "HJ","HJ해외모집단맥주", "SUM_QT", sort)

    // DK
    val DK_Beer_Pop = aggColFtr(DK_Beer, "DIST_NM", "SUP_AMT", "Diageo", "DK모집단수입맥주", "QT", sort)
    val DK_Whi_Pop =  aggColFtr(DK_Whi, "DIST_NM", "SUP_AMT", "Diageo Korea","DK모집단위스키", "QT", sort)
    // HK
    val HK_Beer_Pop = aggColFtr(HK_Beer, "DIST_NM", "SUP_AMT", "HK코리아","HK모집단수입맥주","QT", sort)
    // 통합
    val now_Pop = HJ_Soju_Pop.
      union(HJ_Domestic_Beer_Pop).
      union(HJ_Oversea_Beer_Pop).
      union(DK_Beer_Pop).
      union(DK_Whi_Pop).
      union(HK_Beer_Pop)
    // 이번달 MS 추출
    val now_Res = joinRstDf(Tot, now_Pop, sort)
    now_Res
  }


  def joinBfAfMonthPop(nowRes: DataFrame, bfRes: DataFrame)={
    val mgDf = nowRes.
      join(bfRes, Seq("POP")).
      withColumn("VAR_TOT_QT", ($"NOW_TOT_QT" - $"BF_TOT_QT")/$"BF_TOT_QT").
      withColumn("VAR_TOT_AMT", ($"NOW_TOT_AMT" - $"BF_TOT_AMT")/$"BF_TOT_AMT").
      withColumn("VAR_POP_QT", ($"NOW_POP_QT" - $"BF_POP_QT")/$"BF_POP_QT").
      withColumn("VAR_POP_AMT", ($"NOW_POP_AMT" - $"BF_POP_AMT")/$"BF_POP_AMT").
      withColumn("DIFF_QT_MS", $"NOW_QT_MS" - $"BF_QT_MS").
      withColumn("DIFF_AMT_MS", $"NOW_AMT_MS" - $"BF_AMT_MS")
    val seriesData1M_AMT = mgDf.
      select("POP", "BF_TOT_AMT", "BF_POP_AMT", "BF_AMT_MS", "NOW_TOT_AMT", "NOW_POP_AMT", "NOW_AMT_MS", "VAR_TOT_AMT", "VAR_POP_AMT", "DIFF_AMT_MS").
      toDF("모집단", "전달 전체 모집단 매입금액", "전달 고객사 모집단 매입금액", "전달 MS", "이번달 전체 모집단 매입금액", "이번달 고객사 모집단 매입금액", "이번달 MS", "전체 모집단 증감률", "전체 고객사 모집단 증감률", "MS 차이")

    val seriesData1M_QT = mgDf.
      select("POP", "BF_TOT_QT", "BF_POP_QT", "BF_QT_MS", "NOW_TOT_QT", "NOW_POP_QT", "NOW_QT_MS", "VAR_TOT_QT", "VAR_POP_QT", "DIFF_QT_MS").
      toDF("모집단", "전달 전체 모집단 매입량", "전달 고객사 모집단 매입량", "전달 MS", "이번달 전체 모집단 매입량", "이번달 고객사 모집단 매입량", "이번달 MS", "전체 모집단 증감률", "전체 고객사 모집단 증감률", "MS 차이")

    (seriesData1M_AMT, seriesData1M_QT)
  }

  def mkSheet1(bf1M_Res: DataFrame, now_Res: DataFrame)={

    // 전달 대비 MS차이 및 증감률 추출
    val (seriesData1M_AMT, seriesData1M_QT) = joinBfAfMonthPop(now_Res,bf1M_Res)

    seriesData1M_AMT.orderBy($"모집단").show(5, false)
    seriesData1M_QT.orderBy($"모집단").show(5, false)

    (seriesData1M_QT, seriesData1M_AMT)
  }

  def joinBfAfYearPop(nowRes: DataFrame, bfRes: DataFrame)={
    val seriesData1Y = nowRes.
      join(bfRes, Seq("POP")).
      withColumn("VAR_TOT_QT", ($"NOW_TOT_QT" - $"BF_TOT_QT")/$"BF_TOT_QT").
      withColumn("VAR_TOT_AMT", ($"NOW_TOT_AMT" - $"BF_TOT_AMT")/$"BF_TOT_AMT").
      withColumn("VAR_POP_QT", ($"NOW_POP_QT" - $"BF_POP_QT")/$"BF_POP_QT").
      withColumn("VAR_POP_AMT", ($"NOW_POP_AMT" - $"BF_POP_AMT")/$"BF_POP_AMT").
      withColumn("DIFF_QT_MS", $"NOW_QT_MS" - $"BF_QT_MS").
      withColumn("DIFF_AMT_MS", $"NOW_AMT_MS" - $"BF_AMT_MS")

    val seriesData1Y_AMT = seriesData1Y.
      select("POP", "BF_TOT_AMT", "BF_POP_AMT", "BF_AMT_MS", "NOW_TOT_AMT", "NOW_POP_AMT", "NOW_AMT_MS", "VAR_TOT_AMT", "VAR_POP_AMT", "DIFF_AMT_MS").
      toDF("모집단", "작년 전체 모집단 매입금액", "작년 고객사 모집단 매입금액", "작년 MS", "이번달 전체 모집단 매입금액", "이번달 고객사 모집단 매입금액", "이번달 MS", "전체 모집단 증감률", "전체 고객사 모집단 증감률", "MS 차이")

    val seriesData1Y_QT = seriesData1Y.
      select("POP", "BF_TOT_QT", "BF_POP_QT", "BF_QT_MS", "NOW_TOT_QT", "NOW_POP_QT", "NOW_QT_MS", "VAR_TOT_QT", "VAR_POP_QT", "DIFF_QT_MS").
      toDF("모집단", "작년 전체 모집단 매입량", "작년 고객사 모집단 매입량", "작년 MS", "이번달 전체 모집단 매입량", "이번달 고객사 모집단 매입량", "이번달 MS", "전체 모집단 증감률", "전체 고객사 모집단 증감률", "MS 차이")

    seriesData1Y_AMT.orderBy($"모집단").show(10, false)
    seriesData1Y_QT.orderBy($"모집단").show(10, false)
    (seriesData1Y_AMT, seriesData1Y_QT)
  }

  def mkSheet2(bf1Y_HJ: DataFrame, bf1Y_DK_Beer: DataFrame, bf1Y_DK_Whi: DataFrame, bf1Y_HK: DataFrame)={
    // 작년 전체 고객사별 모집단 매입금액, 매입량 추출

    // HJ
    val bf1Y_HJ_Soju_Tot =  bf1Y_HJ.
      filter($"HJ_MO" === "HJ모집단소주").
      groupBy("HJ_MO").
      agg(sum($"SUM_SUP_AMT") as "BF_TOT_AMT", sum($"SUM_QT") as "BF_TOT_QT").
      withColumnRenamed("HJ_MO", "POP").
      select("POP", "BF_TOT_AMT", "BF_TOT_QT")
    val bf1Y_HJ_Domestic_Beer_Tot =  bf1Y_HJ.
      filter($"CATEGORY" === "맥주").
      filter($"ATT01" === "국내").
      groupBy("HJ_MO").
      agg(sum($"SUM_SUP_AMT") as "BF_TOT_AMT", sum($"SUM_QT") as "BF_TOT_QT").
      withColumnRenamed("HJ_MO", "POP").
      withColumn("POP", lit("HJ국내모집단맥주")).
      select("POP", "BF_TOT_AMT", "BF_TOT_QT")
    val bf1Y_HJ_Oversea_Beer_Tot =  bf1Y_HJ.
      filter($"CATEGORY" === "맥주").
      filter($"ATT01" === "인터내셔널").
      groupBy("HJ_MO").
      agg(sum($"SUM_SUP_AMT") as "BF_TOT_AMT", sum($"SUM_QT") as "BF_TOT_QT").
      withColumnRenamed("HJ_MO", "POP").
      withColumn("POP", lit("HJ해외모집단맥주")).
      select("POP", "BF_TOT_AMT", "BF_TOT_QT")

    // DK
    val bf1Y_DK_Beer_Tot =  aggCol(bf1Y_DK_Beer,"DK모집단수입맥주", "SUP_AMT", "QT","BF")
    val bf1Y_DK_Whi_Tot =  aggCol(bf1Y_DK_Whi, "DK모집단위스키", "SUP_AMT", "QT","BF")
    // HK
    val bf1Y_HK_Beer_Tot = aggCol(bf1Y_HK, "HK모집단수입맥주","SUP_AMT", "QT", "BF")
    // 통합
    val bf1Y_Tot = bf1Y_HJ_Soju_Tot.
      union(bf1Y_HJ_Domestic_Beer_Tot).
      union(bf1Y_HJ_Oversea_Beer_Tot).
      union(bf1Y_DK_Beer_Tot).
      union(bf1Y_DK_Whi_Tot).
      union(bf1Y_HK_Beer_Tot)

    // 작년 해당 고객사의 매입금액, 매입량 추출
    // HJ
    val bf1Y_HJ_Soju_Pop =  bf1Y_HJ.
      filter($"COMPANY" === "HJ").
      filter($"HJ_MO" === "HJ모집단소주").
      groupBy("HJ_MO").
      agg(sum($"SUM_SUP_AMT") as "BF_POP_AMT", sum($"SUM_QT") as "BF_POP_QT").
      withColumnRenamed("HJ_MO", "POP").
      select("POP", "BF_POP_AMT", "BF_POP_QT")
    val bf1Y_HJ_Domestic_Beer_Pop =  bf1Y_HJ.
      filter($"COMPANY" === "HJ").
      filter($"HJ_MO" === "HJ모집단맥주").
      filter($"ATT01" === "국내").
      groupBy("HJ_MO").
      agg(sum($"SUM_SUP_AMT") as "BF_POP_AMT", sum($"SUM_QT") as "BF_POP_QT").
      withColumnRenamed("HJ_MO", "POP").
      withColumn("POP", lit("HJ국내모집단맥주")).
      select("POP", "BF_POP_AMT", "BF_POP_QT")
    val bf1Y_HJ_Oversea_Beer_Pop =  bf1Y_HJ.
      filter($"COMPANY" === "HJ").
      filter($"HJ_MO" === "HJ모집단맥주").
      filter($"ATT01" === "인터내셔널").
      groupBy("HJ_MO").
      agg(sum($"SUM_SUP_AMT") as "BF_POP_AMT", sum($"SUM_QT") as "BF_POP_QT").
      withColumnRenamed("HJ_MO", "POP").
      withColumn("POP", lit("HJ해외모집단맥주")).
      select("POP", "BF_POP_AMT", "BF_POP_QT")

    // DK
    val bf1Y_DK_Beer_Pop =  bf1Y_DK_Beer.
      filter($"DIST_NM" === "Diageo").
      agg(sum($"SUP_AMT") as "BF_POP_AMT", sum($"QT") as "BF_POP_QT").
      withColumn("POP", lit("DK모집단수입맥주")).
      select("POP", "BF_POP_AMT", "BF_POP_QT")
    val bf1Y_DK_Whi_Pop =  bf1Y_DK_Whi.
      filter($"DIST_NM" === "Diageo Korea").
      agg(sum($"SUP_AMT") as "BF_POP_AMT", sum($"QT") as "BF_POP_QT").
      withColumn("POP", lit("DK모집단위스키")).
      select("POP", "BF_POP_AMT", "BF_POP_QT")
    // HK
    val bf1Y_HK_Beer_Pop = bf1Y_HK.
      filter($"DIST_NM" === "HK코리아").
      agg(sum($"SUP_AMT") as "BF_POP_AMT", sum($"QT") as "BF_POP_QT").
      withColumn("POP", lit("HK모집단수입맥주")).
      select("POP", "BF_POP_AMT", "BF_POP_QT")
    // 통합
    val bf1Y_Pop = bf1Y_HJ_Soju_Pop.
      union(bf1Y_HJ_Domestic_Beer_Pop).
      union(bf1Y_HJ_Oversea_Beer_Pop).
      union(bf1Y_DK_Beer_Pop).
      union(bf1Y_DK_Whi_Pop).
      union(bf1Y_HK_Beer_Pop)

    // 작년 MS 추출
    val bf1Y_Res = bf1Y_Tot.
      join(bf1Y_Pop, Seq("POP")).
      withColumn("BF_AMT_MS", $"BF_POP_AMT"/$"BF_TOT_AMT").
      withColumn("BF_QT_MS", $"BF_POP_QT"/$"BF_TOT_QT")
    (bf1Y_Res)
  }
  // main
  def runVerifyData(ethDt: String, flag: String): Unit ={
    val filePathCls = new FilePath(ethDt, flag)
    val ethDtBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethDtBf1m, flag)

    val bf1M_HJ_Soju =  fileFuncCls.rParquet(filePathClsBf1m.hjwMartSPath)
    val bf1M_HJ_Beer =  fileFuncCls.rParquet(filePathClsBf1m.hjwMartBPath)
    val bf1M_HJ_Domestic_Beer = bf1M_HJ_Beer.filter($"PROD_NM" === "국내")
    val bf1M_HJ_Oversea_Beer = bf1M_HJ_Beer.filter($"PROD_NM" === "인터내셔널")
    val now_HJ_Soju =  fileFuncCls.rParquet(filePathCls.hjwMartSPath)
    val now_HJ_Beer =  fileFuncCls.rParquet(filePathCls.hjwMartBPath)
    val now_HJ_Domestic_Beer = now_HJ_Beer.filter($"PROD_NM" === "국내")
    val now_HJ_Oversea_Beer = now_HJ_Beer.filter($"PROD_NM" === "인터내셔널")

    // DK
    val bf1M_DK_Beer = fileFuncCls.rParquet(filePathClsBf1m.dkwMartBPath)
    val bf1M_DK_Whi = fileFuncCls.rParquet(filePathClsBf1m.dkwMartWPath)
    val now_DK_Beer = fileFuncCls.rParquet(filePathCls.dkwMartBPath)
    val now_DK_Whi = fileFuncCls.rParquet(filePathCls.dkwMartWPath)

    // HK
    val bf1M_HK = fileFuncCls.rParquet(filePathClsBf1m.hkwMartPath)
    val now_HK = fileFuncCls.rParquet(filePathCls.hkwMartPath)

    val bf1M_Res = mkSheet1Pre(bf1M_HJ_Soju, bf1M_HJ_Domestic_Beer, bf1M_HJ_Oversea_Beer,bf1M_DK_Beer, bf1M_DK_Whi,bf1M_HK, "BF")
    val now_Res = mkSheet1Pre(now_HJ_Soju, now_HJ_Domestic_Beer, now_HJ_Oversea_Beer, now_DK_Beer, now_DK_Whi, now_HK, "NOW")

    val (seriesData1M_QT, seriesData1M_AMT) = mkSheet1(bf1M_Res, now_Res)
    // 저장 경로 설정
    fileFuncCls.multiSheetXlsx(seriesData1M_QT, "월별 시계열 추이(매입량)", filePathCls.verifyTimePath)
    fileFuncCls.multiSheetXlsx(seriesData1M_AMT, "월별 시계열 추이(매입액)",filePathCls.verifyTimePath)

  }
}

