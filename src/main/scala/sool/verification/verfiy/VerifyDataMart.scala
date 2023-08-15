package sool.verification.verfiy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct, lit, sum}
import sool.common.function.{FileFunc, GetTime}
import sool.common.path.FilePath

class VerifyDataMart(spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val fileFuncCls = new FileFunc(spark)

  // makeVerifyData : HJ/HK 수집확인 및 전달/이번달 거래한 소매상을 통한 검증값 추출
  def makeVerifyData(Group: String, filterCol: String, filterCom: String)(nowDF: DataFrame, bf1mDF: DataFrame): DataFrame = {
    // 전달/이번달 도매상 비율 정보 추출
    val nowDFRes = nowDF.withColumnRenamed("SUM_QT", "QT").withColumnRenamed("COM_RGNO", "BYR_RGNO")
    val bf1mDFRes = bf1mDF.withColumnRenamed("SUM_QT", "QT").withColumnRenamed("COM_RGNO", "BYR_RGNO")
    val nowSupInfo = nowDFRes.agg(countDistinct($"SUP_RGNO") as "NOW_ALL_CNT", sum($"QT") as "NOW_ALL_QT").withColumn("G", lit(Group))
    val supIntersect = bf1mDFRes.select("SUP_RGNO").distinct().intersect(nowDFRes.select("SUP_RGNO").distinct()).agg(countDistinct($"SUP_RGNO") as "INTER_SUP_CNT").withColumn("G", lit(Group))
    val supCntRes = nowSupInfo.join(supIntersect, Seq("G"))
    // 전달/이번달 거래한 소매상의 매입량 비율을 전체, 고객사, 비고객사 별로 값 추출
    val byrIntersect = bf1mDFRes.select("BYR_RGNO").distinct().intersect(nowDFRes.select("BYR_RGNO").distinct())
    val nowInterByrInfo = nowDFRes.join(byrIntersect, Seq("BYR_RGNO")).withColumnRenamed("SUM_QT", "QT").agg(sum($"QT") as "NOW_INTER_QT").withColumn("G", lit(Group))
    val nowInterByrTargetInfo = nowDFRes.join(byrIntersect, Seq("BYR_RGNO")).filter(col(filterCol) === filterCom).agg(sum($"QT") as "NOW_INTER_QT_T").withColumn("G", lit(Group))
    val nowInterByrNonTargetInfo = nowDFRes.join(byrIntersect, Seq("BYR_RGNO")).filter(col(filterCol) =!= filterCom).agg(sum($"QT") as "NOW_INTER_QT_NT").withColumn("G", lit(Group))
    val bfInterByrInfo = bf1mDFRes.join(byrIntersect, Seq("BYR_RGNO")).agg(sum($"QT") as "BF_INTER_QT").withColumn("G", lit(Group))
    val bfInterByrTargetInfo = bf1mDFRes.join(byrIntersect, Seq("BYR_RGNO")).filter(col(filterCol) === filterCom).agg(sum($"QT") as "BF_INTER_QT_T").withColumn("G", lit(Group))
    val bfInterByrNonTargetInfo = bf1mDFRes.join(byrIntersect, Seq("BYR_RGNO")).filter(col(filterCol) =!= filterCom).agg(sum($"QT") as "BF_INTER_QT_NT").withColumn("G", lit(Group))
    // 추출한 값 모두 Join
    val JoinRes = supCntRes.join(nowInterByrInfo, Seq("G")).
      join(nowInterByrTargetInfo, Seq("G")).
      join(nowInterByrNonTargetInfo, Seq("G")).
      join(bfInterByrInfo, Seq("G")).
      join(bfInterByrTargetInfo, Seq("G")).
      join(bfInterByrNonTargetInfo, Seq("G"))
    // Join한 결과로 각각의 비율 값 추출
    val resultDF = JoinRes.withColumn("SUP_PER", $"INTER_SUP_CNT" / $"NOW_ALL_CNT").
      withColumn("NOW_PER", $"NOW_INTER_QT" / $"NOW_ALL_QT").
      withColumn("INTER_PER", $"BF_INTER_QT" / $"NOW_INTER_QT").
      withColumn("TARGET_INTER_PER", $"BF_INTER_QT_T" / $"NOW_INTER_QT_T").
      withColumn("NON_TARGET_INTER_PER", $"BF_INTER_QT_NT" / $"NOW_INTER_QT_NT").
      select("G", "SUP_PER", "NOW_PER", "INTER_PER", "TARGET_INTER_PER", "NON_TARGET_INTER_PER").
      toDF("모집단", "전달대비 도매상 비율", "전달 이번달 공통 소매상의 거래 비율(기준: 업체 수)", "전달 이번달 공통 소매상의 매입량 비율", "전달 이번달 공통 소매상의 고객사 매입량 비율", "전달 이번달 공통 소매상의 비고객사 매입량 비율")

    resultDF
  }

  // makeVerifyDataDK : DK 수집확인 및 전달/이번달 거래한 소매상을 통한 검증 값 추출

  def makeVerifyDataDK(Group: String, filterCol: String, filterCom: String)(nowDF: DataFrame, bf1mDF: DataFrame)(nowBase: DataFrame, bf1mBase: DataFrame): DataFrame = {
    // 전달/이번달 도매상 비율 정보 추출
    val nowDFRes = nowDF.withColumnRenamed("SUM_QT", "QT").withColumnRenamed("COM_RGNO", "BYR_RGNO")
    val bf1mDFRes = bf1mDF.withColumnRenamed("SUM_QT", "QT").withColumnRenamed("COM_RGNO", "BYR_RGNO")
    val nowSupDF = nowDFRes.join(nowBase, Seq("BYR_RGNO", "ITEM")).select("SUP_RGNO").distinct()
    val bfSupDF = bf1mDFRes.join(bf1mBase, Seq("BYR_RGNO", "ITEM")).select("SUP_RGNO").distinct()
    val nowSupCntInfo = nowSupDF.agg(countDistinct($"SUP_RGNO") as "NOW_ALL_CNT").withColumn("G", lit(Group))
    val supIntersect = nowSupDF.intersect(bfSupDF).agg(countDistinct($"SUP_RGNO") as "INTER_SUP_CNT").withColumn("G", lit(Group))
    val nowSupQtInfo = nowDFRes.agg(sum($"QT") as "NOW_ALL_QT").withColumn("G", lit(Group))
    val nowSupInfo = nowSupCntInfo.join(nowSupQtInfo, Seq("G"))
    val supCntRes = nowSupInfo.join(supIntersect, Seq("G"))
    // 전달/이번달 거래한 소매상의 매입량 비율을 전체, 고객사, 비고객사 별로 값 추출
    val byrIntersect = bf1mDFRes.select("BYR_RGNO").distinct().intersect(nowDF.select("BYR_RGNO").distinct())
    val nowInterByrInfo = nowDFRes.join(byrIntersect, Seq("BYR_RGNO")).agg(sum($"QT") as "NOW_INTER_QT").withColumn("G", lit(Group))
    val nowInterByrTargetInfo = nowDFRes.join(byrIntersect, Seq("BYR_RGNO")).filter(col(filterCol) === filterCom).agg(sum($"QT") as "NOW_INTER_QT_T").withColumn("G", lit(Group))
    val nowInterByrNonTargetInfo = nowDFRes.join(byrIntersect, Seq("BYR_RGNO")).filter(col(filterCol) =!= filterCom).agg(sum($"QT") as "NOW_INTER_QT_NT").withColumn("G", lit(Group))
    val bfInterByrInfo = bf1mDFRes.join(byrIntersect, Seq("BYR_RGNO")).agg(sum($"QT") as "BF_INTER_QT").withColumn("G", lit(Group))
    val bfInterByrTargetInfo = bf1mDFRes.join(byrIntersect, Seq("BYR_RGNO")).filter(col(filterCol) === filterCom).agg(sum($"QT") as "BF_INTER_QT_T").withColumn("G", lit(Group))
    val bfInterByrNonTargetInfo = bf1mDFRes.join(byrIntersect, Seq("BYR_RGNO")).filter(col(filterCol) =!= filterCom).agg(sum($"QT") as "BF_INTER_QT_NT").withColumn("G", lit(Group))
    // 추출한 값 모두 Join
    val JoinRes = supCntRes.join(nowInterByrInfo, Seq("G")).
      join(nowInterByrTargetInfo, Seq("G")).
      join(nowInterByrNonTargetInfo, Seq("G")).
      join(bfInterByrInfo, Seq("G")).
      join(bfInterByrTargetInfo, Seq("G")).
      join(bfInterByrNonTargetInfo, Seq("G"))
    // Join한 결과로 각각의 비율 값 추출
    val resultDF = JoinRes.withColumn("SUP_PER", $"INTER_SUP_CNT" / $"NOW_ALL_CNT").
      withColumn("NOW_PER", $"NOW_INTER_QT" / $"NOW_ALL_QT").
      withColumn("INTER_PER", $"NOW_INTER_QT" / $"BF_INTER_QT").
      withColumn("TARGET_INTER_PER", $"NOW_INTER_QT_T" / $"BF_INTER_QT_T").
      withColumn("NON_TARGET_INTER_PER", $"NOW_INTER_QT_NT" / $"BF_INTER_QT_NT").
      select("G", "SUP_PER", "NOW_PER", "INTER_PER", "TARGET_INTER_PER", "NON_TARGET_INTER_PER").
      toDF("모집단", "전달대비 도매상 비율", "전달 이번달 공통 소매상의 거래 비율(기준: 업체 수)", "전달 이번달 공통 소매상의 매입량 비율", "전달 이번달 공통 소매상의 고객사 매입량 비율", "전달 이번달 공통 소매상의 비고객사 매입량 비율")

    resultDF
  }

  // makeComGroupData : 전달/이번달 제조업체 매입량 비율 값 추출
  def makeComGroupData(Group: String, filterCol: String)(nowDF: DataFrame, bf1mDF: DataFrame): DataFrame = {
    // 이번달 제조업체 매입량 비율 추출
    val nowDFRes = nowDF.withColumnRenamed("SUM_QT", "QT").withColumnRenamed("COM_RGNO", "BYR_RGNO")
    val bf1mDFRes = bf1mDF.withColumnRenamed("SUM_QT", "QT").withColumnRenamed("COM_RGNO", "BYR_RGNO")
    val nowGroupRes = nowDFRes.groupBy(filterCol).agg(sum($"QT") as "NOW_QT").withColumn("G", lit(Group))
    val nowAllRes = nowDFRes.agg(sum($"QT") as "NOW_ALL_QT").withColumn("G", lit(Group))
    val nowGroupDF = nowGroupRes.join(nowAllRes, Seq("G")).withColumn("NOW_PER", ($"NOW_QT" / $"NOW_ALL_QT"))
    // 전달 제조업체 매입량 비율 추출
    val bfGroupRes = bf1mDFRes.groupBy(filterCol).agg(sum($"QT") as "BF_QT").withColumn("G", lit(Group))
    val bfAllRes = bf1mDFRes.agg(sum($"QT") as "BF_ALL_QT").withColumn("G", lit(Group))
    val bfGroupDF = bfGroupRes.join(bfAllRes, Seq("G")).withColumn("BF_PER", ($"BF_QT" / $"BF_ALL_QT"))
    // 이번달/전달 매입량 비율 차이값 추출
    val allGroupRes = nowGroupDF.join(bfGroupDF, Seq("G", filterCol), "outer").na.fill(0).
      withColumn("PER_MS", $"NOW_PER" - $"BF_PER").orderBy($"PER_MS".desc).
      select("G", filterCol, "BF_PER", "NOW_PER", "PER_MS").
      toDF("모집단", "고객사", "전달 고객사별 매입량 비율", "이번달 고객사별 매입량 비율", "이번달 전달 매입량 비율 차이")

    allGroupRes

  }


  // main
  def runVerifyDataMart(ethDt: String, flag: String): Unit = {
    val filePathCls = new FilePath(ethDt, flag)
    val ethDtBf1m = getTimeCls.getEthBf1m(ethDt) // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethDtBf1m, flag)

    // 2차 분석 데이터 마트
    val bf1M_baseTag = fileFuncCls.rParquet(filePathClsBf1m.baseTaggedPath).select("SUP_RGNO", "BYR_RGNO", "ITEM").distinct()
    val now_baseTag = fileFuncCls.rParquet(filePathCls.baseTaggedPath).select("SUP_RGNO", "BYR_RGNO", "ITEM").distinct()

    // HJ
    val bf1M_HJ_Soju = fileFuncCls.rParquet(filePathClsBf1m.hjwMartSPath)
    val bf1M_HJ_Beer = fileFuncCls.rParquet(filePathClsBf1m.hjwMartBPath)
    val bf1M_HJ_Domestic_Beer = bf1M_HJ_Beer.filter($"PROD_NM" === "국내")
    val bf1M_HJ_Oversea_Beer = bf1M_HJ_Beer.filter($"PROD_NM" === "인터내셔널")
    val now_HJ_Soju = fileFuncCls.rParquet(filePathCls.hjwMartSPath)
    val now_HJ_Beer = fileFuncCls.rParquet(filePathCls.hjwMartBPath)
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

    val hjsVerifyData = makeVerifyData("HJ모집단소주", "MFG_NM", "HJ")(now_HJ_Soju, bf1M_HJ_Soju)
    val hjbDomesticVerifyData = makeVerifyData("HJ국내모집단맥주", "MFG_NM", "HJ")(now_HJ_Domestic_Beer, bf1M_HJ_Domestic_Beer)
    val hjbOverseaVerifyData = makeVerifyData("HJ해외모집단맥주", "MFG_NM", "HJ")(now_HJ_Oversea_Beer, bf1M_HJ_Oversea_Beer)
    val hkbVerifyData = makeVerifyData("HK모집단수입맥주", "DIST_NM", "HK코리아")(now_HK, bf1M_HK)
    val dkbVerifyData = makeVerifyDataDK("DK모집단수입맥주", "DIST_NM", "Diageo")(now_DK_Beer, bf1M_DK_Beer)(now_baseTag, bf1M_baseTag)
    val dkwVerifyData = makeVerifyDataDK("DK모집단위스키", "DIST_NM", "Diageo Korea")(now_DK_Whi, bf1M_DK_Whi)(now_baseTag, bf1M_baseTag)

    val allVerifyData = hjsVerifyData.union(hjbDomesticVerifyData).union(hjbOverseaVerifyData).union(dkbVerifyData).union(dkwVerifyData).union(hkbVerifyData)
    allVerifyData.show(100, false)
    fileFuncCls.multiSheetXlsx(allVerifyData, "전달 이번달 수집량 비교 및 소매상 거래 검증", filePathCls.verifyMartPath)

    val hjsComData = makeComGroupData("HJ모집단소주", "MFG_NM")(now_HJ_Soju, bf1M_HJ_Soju)
    val hjbDomesticComData = makeComGroupData("HJ국내모집단맥주", "MFG_NM")(now_HJ_Domestic_Beer, bf1M_HJ_Domestic_Beer)
    val hjbOverseaComData = makeComGroupData("HJ해외모집단맥주", "MFG_NM")(now_HJ_Oversea_Beer, bf1M_HJ_Oversea_Beer)
    val dkbComData = makeComGroupData("DK모집단수입맥주", "DIST_NM")(now_DK_Beer, bf1M_DK_Beer)
    val dkwComData = makeComGroupData("DK모집단위스키", "DIST_NM")(now_DK_Whi, bf1M_DK_Whi)
    val hkComData = makeComGroupData("HK모집단수입맥주", "DIST_NM")(now_HK, bf1M_HK)

    val allComResult = hjsComData.union(hjbDomesticComData).union(hjbOverseaComData).union(dkbComData).union(dkwComData).union(hkComData)
    allComResult.show(100, false)
    fileFuncCls.multiSheetXlsx(allComResult, "전달 이번달 제조업체 매입량 비율", filePathCls.verifyMartPath)
  }
}