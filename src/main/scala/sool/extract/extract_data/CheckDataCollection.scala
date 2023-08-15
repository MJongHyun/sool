package sool.extract.extract_data

import org.apache.spark.sql.functions.{lit, udf}
import java.time._
import java.time.format.DateTimeFormatter
import sool.common.function.{FileFunc, GetTime}
import sool.common.path.FilePath
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


// Zeppelin 1.Extract 1-1 Extract Ethanol
class CheckDataCollection(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  /* 필요 클래스 선언 */
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()

  def getDate(thisMonth:String, date_type:String, period:Int = 1):String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dtfm = DateTimeFormatter.ofPattern("yyyyMM")
    val dkfm = DateTimeFormatter.ofPattern("yy-MM")
    val date = LocalDate.parse(thisMonth+"01", dtf)

    if (date_type(0) == 'B' && date_type(1) == 'M'){
      val bfMonth = date.minusMonths(period).format(dtfm)
      bfMonth
    }else if (date_type(0) == 'A' && date_type(1) == 'M'){
      val afMonth = date.plusMonths(period).format(dtfm)
      afMonth
    }else if (date_type(0) == 'B' && date_type(1) == 'Y'){
      val bfYear = date.minusYears(period).format(dtfm)
      bfYear
    }else if (date_type(0) == 'A' && date_type(1) == 'Y'){
      val afYear = date.plusYears(period).format(dtfm)
      afYear
    }else if (date_type == "DK"){
      val dkDate = date.plusMonths(6).format(dkfm).split("-")
      s"F${dkDate(0)}P${dkDate(1)}"
    }else{
      ""
    }
  }

  //  주류 데이터 수집량 시계열 추이
  def ethanolTimeTrend(ethDt : String, flag:String): Unit = {
    val filePathCls = new FilePath(ethDt, flag)
    val (ethBf2M, ethBf1M) = (getDate(ethDt, "BM", 2), getDate(ethDt, "BM"))

    // 시계열 분석에 필요한 분석연월 추출 (기준 : 분석연월 기준 분기, 작년 기준 분기)
    val dateLs = List(getDate(ethDt, "BM", 1), getDate(ethDt, "BM", 2), getDate(ethDt, "BY", 1), getDate(ethBf1M, "BY", 1), getDate(ethBf2M, "BY", 1))


    // 필요한 성분만 추출 (날짜, 매입금액, 공급업체 사업자, 매입업체 사업자)
    val dataCols = Seq("WR_DT", "SUP_AMT", "SUP_RGNO", "BYR_RGNO").map(col)

    // 시계열 분석연월에 해당하는 거래데이터 데이터
    var CollectionDtiData = fileFuncCls.rParquet(filePathCls.dtiPath).select(dataCols: _*)
    dateLs.foreach(ym => CollectionDtiData = CollectionDtiData.union(fileFuncCls.rParquet(s"s3a://sool/Base/${ym}/Data/dti.parquet").select(dataCols: _*)))

    // 시계열 분석연월에 해당하는 주류 데이터
    var CollectionEthData = fileFuncCls.rParquet(filePathCls.dtiEthPath).select(dataCols: _*)
    dateLs.foreach(ym => CollectionEthData = CollectionEthData.union(fileFuncCls.rParquet(s"s3a://sool/Base/${ym}/Data/dtiEthanol.parquet").select(dataCols: _*)))

    // 분석연월 별 거래데이터 거래건 수 추출
    val collectionDtiInfo = CollectionDtiData.groupBy("WR_DT").count().toDF("WR_DT", "DTI_COUNT")

    // 분석연월 별 주류데이터 거래건 수 및 공급자 수, 매입자 수, 매입금액, 공급자 대비 거래건 수 비율 추출
    val collectionEthInfo = CollectionEthData.groupBy("WR_DT").count().toDF("WR_DT", "ETH_COUNT")
    val collectionEthInfo2 = CollectionEthData.groupBy("WR_DT").
      agg(sum("SUP_AMT").as("AMT"), countDistinct("SUP_RGNO").as("SUP"), countDistinct("BYR_RGNO").as("BYR"))
    val totalEthInfo = collectionEthInfo.join(collectionEthInfo2, Seq("WR_DT")).
      withColumn("ETH_DEAL_AVG", $"ETH_COUNT" / $"SUP")

    // 전체 데이터 추출
    val collectionDF = collectionDtiInfo.join(totalEthInfo, Seq("WR_DT")).
      withColumn("ETH_DEAL_PER", $"ETH_COUNT" / $"DTI_COUNT").
      select('WR_DT, 'DTI_COUNT, 'ETH_COUNT, 'ETH_DEAL_PER, 'SUP, 'BYR, 'AMT, 'ETH_DEAL_AVG).
      toDF("날짜", "수집된 수", "추출한 주류데이터 거래건 수", "거래데이터 거래건금계산서 대비 주류데이터 거래건 수 비율", "공급자 수", "매입자 수", "매입금액", "공급자 대비 주류데이터 거래 건 수").
      orderBy($"날짜")

    val collectDFparquet = collectionDF.withColumn("WR_DT", lit(ethDt)).toDF("DT", "KETA_DATA_CN", "ETH_DATA_CN", "ETH_DATA_PER", "SUP_CN", "BYR_CN", "BYR_AMT", "ETH_DEAL_PER", "WR_DT")
    collectionDF.show(false)

    fileFuncCls.multiSheetXlsx(collectionDF, "데이터 수집량 시계열 추이", filePathCls.dataCollectPath)
    collectDFparquet.repartition(1).write.parquet(filePathCls.dataCollectIssuePath)
  }

  // 신규 공급업체 검토
  def newSupCheck(ethDt : String, flag: String): Unit ={
    val ethBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)
    // 전달, 이번달 주류데이터 추출
    val ethBF1M = fileFuncCls.rParquet(filePathClsBf1m.dtiEthPath)
    val ethNow = fileFuncCls.rParquet(filePathCls.dtiEthPath)
    // [2021.07.12] 전달까지 추가한 도매상 추가
    val ethVendor = fileFuncCls.rParquet(filePathClsBf1m.vendorPath)
    // [2021.07.12] 전체기간 신규도매상인 경우 "Y" 라는 결과 추가
    val newVendor = ethNow.select("SUP_RGNO").distinct().except(ethVendor).withColumn("NEW", lit("Y"))

    // 바코드를 제거한 아이템 추출
    val extractName = udf((rawItem: String)=>{
      val ItemReplace = rawItem.replace(":", ";").replace(",", ";")
      val ItemSplit = ItemReplace.split(";")
      val ItemRes = ItemSplit.slice(1, ItemSplit.length).mkString(";")
      ItemRes
    })

    // 지난달 주류데이터와 비교하여 신규로 추가된 공급업체 추출
    val addSup = ethNow.select("SUP_RGNO").distinct.except(ethBF1M.select("SUP_RGNO").distinct)

    val wStand = Window.partitionBy("SUP_RGNO").orderBy(desc("SUP_AMT"))
    val wAddr = Window.partitionBy("SUP_RGNO").orderBy($"SUP_NM".desc, $"SUP_ADDR".desc)  // [2021.07.12] 사업자의 상호명, 주소 추출
    val addSupInfo = ethNow.select("SUP_RGNO", "SUP_NM", "SUP_ADDR").distinct(). // [2021.07.12] 전달대비 신규도매상 중, 최초 신규 도매상이 아닌 경우 "N" 라는 결과 추가
      na.fill("").
      withColumn("RANK", rank.over(wAddr)).
      filter($"RANK" === 1).
      drop("RANK").
      join(addSup, Seq("SUP_RGNO")).
      join(newVendor, Seq("SUP_RGNO"), "left_outer").
      withColumn("NEW", when($"NEW".isNull, lit("N")).otherwise($"NEW"))

    val addSupItem = ethNow.
      join(addSupInfo.select("SUP_RGNO"), Seq("SUP_RGNO")).
      groupBy($"SUP_RGNO", $"ITEM_NM").
      agg(sum($"SUP_AMT") as "SUP_AMT").
      withColumn("Rank", rank.over(wStand)).filter($"Rank" <= 3).
      withColumn("ITEM_NM", extractName($"ITEM_NM")).
      groupBy($"SUP_RGNO").
      agg(collect_list("ITEM_NM") as "ITEM").
      withColumn("ITEM", concat_ws(", ", $"ITEM"))

    val resOnlyNow = ethNow.
      groupBy("SUP_RGNO").
      agg(sum("SUP_AMT").as("AMT")).
      join(addSupInfo, Seq("SUP_RGNO")).
      join(addSupItem, Seq("SUP_RGNO")).
      select("SUP_RGNO", "SUP_NM", "SUP_ADDR", "AMT", "ITEM", "NEW").
      toDF("사업자", "상호명", "주소", "이번달 매입금액", "거래 아이템", "신규여부").
      withColumn("비고", lit("")).
      orderBy($"이번달 매입금액".desc)

    val resOnlyNowParquet = resOnlyNow.
      select("사업자", "상호명", "이번달 매입금액", "거래 아이템", "비고").
      withColumn("WR_DT", lit(ethDt)).
      toDF("SUP_RGNO", "SUP_NM", "SUP_AMT", "DEAL_ITEM", "REMARK", "WR_DT")


    println("============================================")
    println("공급업체 추가 (이번달만 존재) : " + resOnlyNow.count)

    resOnlyNow.show(100, false)

    fileFuncCls.multiSheetXlsx(resOnlyNow, "공급업체 추가 (전달대비 신규도매상)", filePathCls.dataCollectPath)
    resOnlyNowParquet.repartition(1).write.parquet(filePathCls.dataCollectSupPath)
  }

  //  누락된 도매상 정보 추출
  def missingVenCom(ethDt : String, flag: String): Unit ={
    val ethBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)
    // 바코드를 제거한 아이템 추출
    val extractName = udf((rawItem: String)=>{
      val ItemReplace = rawItem.replace(":", ";").replace(",", ";")
      val ItemSplit = ItemReplace.split(";")
      val ItemRes = ItemSplit.slice(1, ItemSplit.length).mkString(";")
      ItemRes
    })

    val (ethBf2M, ethBf1M) = (getDate(ethDt, "BM", 2), getDate(ethDt, "BM"))
    // 이번달 주류데이터 추출
    val ethNow = fileFuncCls.rParquet(filePathCls.dtiEthPath)
    // 분석연월에 해당하는 거래데이터 데이터
    val dtiNow  = fileFuncCls.rParquet(filePathCls.dtiPath)
    val ethBF1M = fileFuncCls.rParquet(filePathClsBf1m.dtiEthPath)
    // 지난달 2차 분석 마트데이터
    val baseTagBF1M = fileFuncCls.rParquet(filePathClsBf1m.baseTaggedPath)
    // 아이템 마스터에서 분석 모집단에 해당하는 아이템만 추출
    val BfItemInfo = fileFuncCls.rParquet(filePathClsBf1m.ItemPath).
      filter($"memo".contains("HK") || $"HJ_MO".isNotNull || $"DK_MO".isNotNull).select('ITEM)

    // 분석 모집단에 해당하는 거래만 한 매입업체 추출
    val BfItemBase = baseTagBF1M.
      join(BfItemInfo, Seq("ITEM")).
      select('BYR_RGNO, 'ITEM, 'SUP_AMT)

    // 저번 달 주류 데이터와 이번 달 주류 데이터를 비교하여 이번 달 주류 데이터에 없는 누락 공급업체 추출
    val delSup = ethBF1M.
      select("SUP_RGNO").
      distinct.
      except(ethNow.select("SUP_RGNO").distinct)

    // 누락 공급업체의 이번 달 거래데이터 내 존재 여부
    val ExistSup = delSup.
      join(dtiNow, Seq("SUP_RGNO")).
      select('SUP_RGNO).distinct().
      withColumn("EXIST_SUP_KETA", lit("Y"))

    // 누락 공급업체 대표 사업자명 추출
    val wName = Window.partitionBy("SUP_RGNO").orderBy("SUP_NM")
    val desSupInfo = ethBF1M.
      join(delSup, Seq("SUP_RGNO")).
      select('SUP_RGNO, 'SUP_NM).
      distinct().
      withColumn("Rank", rank.over(wName)).filter($"Rank" === 1).drop("Rank")

    // 누락 공급업체의 저번 달 주류 매입금액 및 저번 달 주류 거래한 소매업체 수
    val EthBf1MInfo = ethBF1M.
      drop("SUP_NM").
      join(desSupInfo, Seq("SUP_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(sum($"SUP_AMT") as "BF_ETH_AMT", countDistinct($"BYR_RGNO") as "BF_ETH_RET_CN")

    // 누락 공급업체와 저번 달 거래한 소매업체들
    val EthBf1MByr = ethBF1M.
      drop("SUP_NM").
      join(desSupInfo, Seq("SUP_RGNO")).
      select('SUP_RGNO, 'SUP_NM, 'BYR_RGNO).
      distinct()

    // 누락 공급업체와 저번 달 주류 거래한 소매업체(들)의 저번 달 주류 데이터 매입금액
    val EthBf1ByrInfo = ethBF1M.
      select('BYR_RGNO, 'SUP_AMT).
      join(EthBf1MByr, Seq("BYR_RGNO")).
      groupBy("SUP_RGNO", "SUP_NM").
      agg(sum("SUP_AMT") as "BF_ETH_RET_AMT")

    // 누락 공급업체와 저번 달 주류 거래한 소매업체(들)의 이번 달 주류 데이터 매입금액 및 소매업체(들)수
    val EthNowByrInfo = ethNow.
      select('BYR_RGNO, 'SUP_AMT).
      join(EthBf1MByr, Seq("BYR_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(countDistinct($"BYR_RGNO") as "NOW_ETH_RET_CN", sum($"SUP_AMT") as "NOW_ETH_RET_AMT")

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 거래데이터에 존재하는 소매업체 수
    val dtiInfo = dtiNow.
      select('BYR_RGNO).
      join(EthBf1MByr, Seq("BYR_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(countDistinct($"BYR_RGNO") as "NOW_KETA_RET_CN")

    // 공급업체 기준으로 공급 금액이 높은 순으로 아이템 확인
    val wStand = Window.partitionBy("SUP_RGNO").orderBy(desc("SUP_AMT"))

    // 누락 공급업체와 저번 달 주류 거래한 소매업체(들)의 이번 달 주류 거래 아이템
    val EthNowItem = ethNow.
      drop("SUP_RGNO", "SUP_NM").
      join(EthBf1MByr, Seq("BYR_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM", $"ITEM_NM").
      agg(sum($"SUP_AMT") as "SUP_AMT").
      withColumn("Rank", rank.over(wStand)).filter($"Rank" <= 3).
      withColumn("ITEM_NM", extractName($"ITEM_NM")).
      select('SUP_RGNO, 'SUP_NM, 'ITEM_NM).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(collect_list("ITEM_NM") as "NOW_ITEM_INFO").
      withColumn("NOW_ITEM_INFO", concat_ws(", ", $"NOW_ITEM_INFO"))

    // 누락 공급업체와 저번 달 주류 거래한 소매업체(들)의 저번 달 주류 거래 아이템
    val EthBfItem = ethBF1M.
      drop("SUP_RGNO", "SUP_NM").
      join(EthBf1MByr, Seq("BYR_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM", $"ITEM_NM").
      agg(sum($"SUP_AMT") as "SUP_AMT").
      withColumn("Rank", rank.over(wStand)).filter($"Rank" <= 3).
      withColumn("ITEM_NM", extractName($"ITEM_NM")).
      select('SUP_RGNO, 'SUP_NM, 'ITEM_NM).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(collect_list("ITEM_NM") as "BF_ITEM_INFO").
      withColumn("BF_ITEM_INFO", concat_ws(", ", $"BF_ITEM_INFO"))

    // 이번 달에 존재하지 않는 소매상 정보 추출
    val ByrName = Window.partitionBy("BYR_RGNO").orderBy("BYR_NM")

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)
    val NowExceptRgno = EthBf1MByr.
      join(ethNow.select('BYR_RGNO, 'BYR_NM, 'SUP_AMT), Seq("BYR_RGNO"), "left_outer").
      filter($"BYR_NM".isNull).
      select('SUP_RGNO, 'SUP_NM, 'BYR_RGNO)

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)의 대표 사업자명 설정
    val NowExceptInfo = ethBF1M.
      select('BYR_RGNO, 'BYR_NM).
      withColumn("Rank", rank.over(ByrName)).filter($"Rank" === 1).drop("Rank").
      distinct().
      join(NowExceptRgno, Seq("BYR_RGNO"))

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)의 저번 달 매입금액
    val NowExceptAmt = ethBF1M.
      select('BYR_RGNO, 'SUP_AMT).
      join(NowExceptInfo, Seq("BYR_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM", $"BYR_RGNO", $"BYR_NM").
      agg(sum($"SUP_AMT") as "SUP_AMT")

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)의 저번 달 분석 모집단 거래액
    val BfExceptMoAmt = NowExceptInfo.
      join(BfItemBase, Seq("BYR_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(sum($"SUP_AMT") as "NOT_EXIST_RET_AMT")

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)별 저번 달 분석 모집단 거래액
    val BfExceptByrMoAmt = NowExceptInfo.
      join(BfItemBase, Seq("BYR_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM", $"BYR_NM").
      agg(sum($"SUP_AMT") as "EXCEPT_AMT")

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)의 저번 달 분석 모집단 아이템별 매입금액
    val BfExceptMoItem = NowExceptInfo.
      join(BfItemBase, Seq("BYR_RGNO")).
      groupBy($"SUP_RGNO", $"SUP_NM", $"ITEM").
      agg(sum($"SUP_AMT") as "EXCEPT_AMT")

    val ExceptStand = Window.partitionBy("SUP_RGNO").orderBy(desc("EXCEPT_AMT"))

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)별 저번 달 분석 모집단 거래액 TOP 5
    val BfExceptByrRank = BfExceptByrMoAmt.
      withColumn("Rank", rank.over(ExceptStand)).
      filter($"Rank" <= 5).
      withColumn("NOT_EXIST_RET_INFO", concat_ws(" : ",$"BYR_NM", $"EXCEPT_AMT")).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(collect_list("NOT_EXIST_RET_INFO") as "NOT_EXIST_RET_INFO").
      withColumn("NOT_EXIST_RET_INFO", concat_ws(", ", $"NOT_EXIST_RET_INFO"))

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)의 저번 달 분석 모집단 아이템별 매입금액 TOP 5
    val BfExceptItemRank = BfExceptMoItem.
      withColumn("Rank", rank.over(ExceptStand)).
      filter($"Rank" <= 5).
      withColumn("NOT_EXIST_ITEM_INFO", concat_ws(" : ",$"ITEM", $"EXCEPT_AMT")).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(collect_list("NOT_EXIST_ITEM_INFO") as "NOT_EXIST_ITEM_INFO").
      withColumn("NOT_EXIST_ITEM_INFO", concat_ws(", ", $"NOT_EXIST_ITEM_INFO"))

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)의 저번 달 매입금액 TOP 5 인 소매업체들만 추출
    val ByrExceptSupAmtTop5 = NowExceptAmt.
      withColumn("Rank", rank.over(wStand)).
      filter($"Rank" <= 5).
      select('SUP_RGNO, 'SUP_NM, 'BYR_NM).
      groupBy($"SUP_RGNO", $"SUP_NM").
      agg(collect_list("BYR_NM") as "NOT_EXIST_RET_NAME").
      withColumn("NOT_EXIST_RET_NAME", concat_ws(", ", $"NOT_EXIST_RET_NAME"))

    // 누락 공급업체와 저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들) 관련 최종 테이블 생성
    val ByrExceptInfo = ByrExceptSupAmtTop5.
      join(BfExceptMoAmt, Seq("SUP_RGNO", "SUP_NM"), "left_outer").na.fill(0).
      join(BfExceptByrRank, Seq("SUP_RGNO", "SUP_NM"), "left_outer").
      join(BfExceptItemRank, Seq("SUP_RGNO", "SUP_NM"), "left_outer").
      na.fill("")

    // 전체 데이터 추출
    val TotalInfoPre = EthBf1MInfo.
      join(dtiInfo, Seq("SUP_RGNO", "SUP_NM"), "left_outer").
      join(EthBf1ByrInfo, Seq("SUP_RGNO", "SUP_NM"), "left_outer").na.fill(0).
      join(EthNowByrInfo, Seq("SUP_RGNO", "SUP_NM"), "left_outer").na.fill(0).
      join(ExistSup, Seq("SUP_RGNO"), "left_outer").na.fill("N").
      join(EthBfItem, Seq("SUP_RGNO", "SUP_NM"), "left_outer").
      join(EthNowItem, Seq("SUP_RGNO", "SUP_NM"), "left_outer").
      join(ByrExceptInfo, Seq("SUP_RGNO", "SUP_NM"), "left_outer").na.fill("").
      withColumn("BF_NOW_RET_CN", $"NOW_ETH_RET_CN"/$"BF_ETH_RET_CN").
      withColumn("BF_NOW_ETH_RET_AMT_PER", $"NOW_ETH_RET_AMT"/$"BF_ETH_RET_AMT").
      withColumn("REMARK", lit("")).
      orderBy($"BF_ETH_AMT".desc)

    // 컬럼 순서 정리
    val TotalInfo = TotalInfoPre.select(
      "SUP_RGNO", "SUP_NM", "EXIST_SUP_KETA",
      "BF_ETH_AMT", "BF_ETH_RET_AMT", "NOW_ETH_RET_AMT",
      "BF_NOW_ETH_RET_AMT_PER", "BF_ETH_RET_CN", "NOW_ETH_RET_CN",
      "BF_NOW_RET_CN", "BF_ITEM_INFO", "NOW_ITEM_INFO",
      "NOW_KETA_RET_CN", "NOT_EXIST_RET_NAME", "NOT_EXIST_RET_INFO",
      "NOT_EXIST_ITEM_INFO", "NOT_EXIST_RET_AMT", "REMARK")

    // 엑셀용, 한글 컬럼명
    val TotalInfoHangulCols = TotalInfo.toDF(
      "사업자", "사업자명", "이번 달 거래데이터 내 존재여부",
      "저번 달 주류 매입금액", "저번 달 주류 거래한 소매업체(들)의 저번 달 주류 매입금액", "저번 달 주류 거래한 소매업체(들)의 이번 달 주류 매입금액",
      "저번 달 주류 거래한 소매업체(들)의 저번 달 주류 매입금액 대비 저번 달 주류 거래한 소매업체(들)의 이번 달 주류 매입금액 비율", "저번 달 주류 거래한 소매업체 수", "저번 달 주류 거래한 소매업체 중 이번 달 주류 거래한 소매업체 수",
      "저번 달 주류 거래한 소매업체 수 대비 저번 달 주류 거래한 소매업체 중 이번 달 주류 거래한 소매업체 수 비율", "저번 달 주류 거래한 소매업체(들)의 저번 달 주류 거래 아이템", "저번 달 주류 거래한 소매업체(들)의 이번 달 주류 거래 아이템",
      "저번 달 주류 거래한 소매업체 중 이번 달 거래데이터에 존재하는 소매업체 수", "저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들) ", "저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)별 저번 달 분석 모집단 매입금액",
      "저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)의 저번 달 분석 모집단 아이템별 매입금액", "저번 달 주류 거래한 소매업체 중 이번 달 주류 데이터에 존재하지 않는 소매업체(들)의 저번 달 분석 모집단 매입금액", "비고")

    println("누락된 도매상 수 : " + delSup.count())
    //    TotalInfoHangulCols.show(100, false)

    // parquet용, 영문 컬럼명
    val TotalInfoForParquet = TotalInfo.withColumn("WR_DT", lit(ethDt))

    fileFuncCls.multiSheetXlsx(TotalInfoHangulCols,  "공급업체 누락 (지난달만 존재)", filePathCls.dataCollectPath)
    TotalInfoForParquet.repartition(1).write.parquet(filePathCls.missingSupPath)
  }

  //  (필요시) 공급업체 전달만 존재하는 원인 파악
  def checkSupDeal(ethDt : String, ckRgno : String, flag: String): Unit = {
    val ethBf1m = getTimeCls.getEthBf1m(ethDt) // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)
    val (ethBf2M, ethBf1M) = (getDate(ethDt, "BM", 2), getDate(ethDt, "BM"))
    // 지난 달, 이 공급업체와 유흥거래를 한 매입업체 추출
    val dtiBFch3 = spark.read.parquet(filePathClsBf1m.basePath)
    val bfRetail = dtiBFch3.filter('SUP_RGNO === ckRgno).select('BYR_RGNO).distinct

    // 분석연월에 해당하는 거래데이터 데이터
    val dtiNow = spark.read.parquet(filePathCls.dtiPath)
    // 전달, 이번달 주류데이터 추출
    val ethBF1M = fileFuncCls.rParquet(filePathClsBf1m.dtiEthPath)
    val ethNow = fileFuncCls.rParquet(filePathCls.dtiEthPath)

    val nowDtiByr = dtiNow.join(bfRetail, Seq("BYR_RGNO")).select('BYR_RGNO).distinct
    val nowEthByr = ethNow.join(bfRetail, Seq("BYR_RGNO")).select('BYR_RGNO).distinct()

    val delByrInfo = ethBF1M.join(bfRetail.except(nowDtiByr), Seq("BYR_RGNO")).groupBy($"BYR_RGNO", $"BYR_NM").agg(sum($"SUP_AMT") as "AMT").orderBy($"AMT".desc)

    println("============================================")
    println("이번달 존재하는 소매상의 거래현황")
    ethNow.join(bfRetail, Seq("BYR_RGNO")).groupBy($"ITEM_NM").agg(sum($"SUP_AMT") as "AMT").orderBy($"AMT".desc).show(10, false)

    println("============================================")
    println("과거 공급업체 거래현황")
    ethBF1M.filter('SUP_RGNO === ckRgno).groupBy("ITEM_NM").agg(sum("SUP_AMT")).show(10, false)

    println("============================================")
    println("현재 거래데이터에서 존재하는 결과")
    dtiNow.filter('SUP_RGNO === ckRgno).
      select('SUP_RGNO, 'SUP_NM, 'ITEM_QT, 'ITEM_SZ, 'ITEM_NM).show(10, false)

    println("============================================")
    println("지난달 유흥거래를 한 소매상들 중에서 이번달 거래데이터에 존재하는 업체 수 : " + nowDtiByr.count)

    println("============================================")
    println("전달 유흥거래를 한 소매상 수 : " + bfRetail.count)
    println("전달 유흥거래를 한 소매상 중에서 이번달에 주류거래를 한 소매상 수 : " + nowEthByr.count)
    println("이번달 주류거래 없는 소매상 수 : " + (bfRetail.count.toInt - nowEthByr.count.toInt))

    println("============================================")
    println("이번달 거래데이터에 존재하지 않는 유흥거래 소매상 정보")
    delByrInfo.show(10, false)
  }

  def dataCollet(ethDt : String, flag: String): Unit ={
    // 데이터수집량_공급업체이슈, data_clctn_issue
    val ethatt = ethanolTimeTrend(ethDt, flag)
    // 데이터수집량_공급업체이슈, new_sup_issue
    val newsupc = newSupCheck(ethDt, flag)
    // 데이터수집량_공급업체이슈, missing_sup_issue
    val missvencom = missingVenCom(ethDt, flag)
    }
}
