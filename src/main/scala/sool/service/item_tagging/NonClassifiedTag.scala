package sool.service.item_tagging

import sool.common.function.{FileFunc, GetTime}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, when, udf, _}
import sool.common.path.FilePath


class NonClassifiedTag(spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._
  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val fileFuncCls = new FileFunc(spark)

  // 영남이면서 참17미분류, 참저도주미분류 값 변경
  def yeongNamUdf() = udf((item: String, area : String) => {
    item match {
      case b1 if(item == "참17미분류" && area == "Y") => "맛있는참오리지널병360"
      case b2 if(item == "참17미분류" && area == "N") => "참이슬후레쉬병360"

      case c1 if(item == "참저도주미분류" && area == "Y") => "맛있는참병360"
      case c2 if(item == "참저도주미분류" && area == "N") => "참이슬후레쉬병360"

      case _ => item
    }
  })

  def sudoUdf() = udf((item: String, area: String) => {
    // 수도권이면서 참오리지널미분류, 참이슬미분류 값 변경
    item match {

      case a1 if (item == "참오리지널미분류" && area == "Y") => "참이슬클래식병360"
      case a2 if (item == "참오리지널미분류" && area == "N") => "맛있는참오리지널병360"

      case d1 if (item == "참이슬미분류" && area == "Y") => "참이슬클래식병360"
      case d2 if (item == "참이슬미분류" && area == "N") => "참이슬후레쉬병360"

      case _ => item
    }
  })

  def itemChangeUdf()= udf((supRgno: String, name: String, itemSz: String, item: String) => {
    // [2022.01.27] 도매상 "구산주류(2298123864)" 업체 오기입 값(클라우드생드래프트병500) 변경
      item match {
        case error if (supRgno == "2298123864" && name == "클라우드생드래프트소" && itemSz == "500.0" && item == "클라우드생드래프트병500") => "클라우드생드래프트병330"
        case _ => item
      }
  })

  def reMatchingCom(comMaster: DataFrame)={
    val unCateChamSidoDF = List("부산광역시", "대구광역시", "울산광역시", "경상남도", "경상북도").toDF("ADDR1").withColumn("YN", lit("Y")) // 영남권
    val chamSidoDF = List("서울특별시", "경기도").toDF("ADDR1").withColumn("YN", lit("Y")) // 서울, 경기

    val unCateChamComDF = comMaster.na.fill("", Seq("ADDR1")). // 주소 Null도 재태깅
      join(unCateChamSidoDF, Seq("ADDR1"), "left").na.fill("N", Seq("YN")).
      select('COM_RGNO as "BYR_RGNO", 'YN).distinct // 영남지역이면 Y, 아니면 N

    val chamComDF = comMaster.na.fill("", Seq("ADDR1")). // 주소 Null도 재태깅
      join(chamSidoDF, Seq("ADDR1"), "left").na.fill("N", Seq("YN")).
      select('COM_RGNO as "BYR_RGNO", 'YN).distinct // 서울, 경기지역이면 Y, 아니면 N
    (unCateChamComDF, chamComDF)
  }

  def chngData(baseItemDF: DataFrame, unCateChamComDF: DataFrame, chamComDF: DataFrame)={
    val areaYeongNamUDF = yeongNamUdf()
    val areaSudoUDF = sudoUdf()
    val baseCol = Seq("ISS_ID", "ITEM_NO", "SUP_RGNO", "SUP_NM", "SUP_ADDR", "BYR_RGNO", "BYR_NM", "BYR_ADDR", "ITEM_NM", "NAME", "ITEM", "ITEM_SZ",
      "ITEM_QT", "ITEM_UP", "SUP_AMT", "BARCODE", "CHANNEL", "TYPE", "TAX_AMT", "WR_DT", "DATE").map(col) // [2020.10.05] AG
    val yeongNamCham = List("참저도주미분류", "참17미분류") // 영남권 해당 아이템
    val suDoCham = List("참오리지널미분류", "참이슬미분류") // 수도권 해당 아이템
    val baseUnCateChamDF = baseItemDF.filter('EXIST.isNotNull && 'ITEM.isin(yeongNamCham:_*)). // 변경할 데이터
      join(unCateChamComDF, Seq("BYR_RGNO"), "left"). // 영남지방 여부 부착
      withColumn("ITEM", areaYeongNamUDF('ITEM, 'YN)). // 제품 재태깅
      select(baseCol:_*)

    val baseChamDF = baseItemDF.filter('EXIST.isNotNull && 'ITEM.isin(suDoCham:_*)). // 변경할 데이터
      join(chamComDF, Seq("BYR_RGNO"), "left"). // 서울, 경기도 여부 부착
      withColumn("ITEM", areaSudoUDF('ITEM, 'YN)). // 제품 재태깅
      select(baseCol:_*)
    (baseUnCateChamDF, baseChamDF)
  }

  def miniNumTest(baseDf: DataFrame, baseFinalDf: DataFrame)= {
    if (baseDf.count() == baseFinalDf.count()) {
      println("Unit-test (Check Number of Lines) : true")
      baseDf.count() == baseFinalDf.count()
    }
    else {
      println(s"${getTimeCls.getTimeStamp()}, 데이터 개수가 다릅니다. 다시 확인해주세요")
      baseDf.count() == baseFinalDf.count()
    }
  }

  def miniTest(baseFinalDf: DataFrame, unCateCham: DataFrame)= {
    if (baseFinalDf.join(unCateCham, Seq("ITEM")).count == 0) {
      println("Unit-test (미분류 완료) : true")
      baseFinalDf.join(unCateCham, Seq("ITEM")).count == 0
    }
    else {
      println(s"${getTimeCls.getTimeStamp()}, 데이터 개수가 다릅니다. 다시 확인해주세요")
      baseFinalDf.join(unCateCham, Seq("ITEM")).count == 0
    }
  }

  // main
  def runItemReMat(ethDt: String, flag: String): Unit ={
    val filePathCls = new FilePath(ethDt, flag)

    val cumulDf = fileFuncCls.rParquet(filePathCls.cumulNminfoDfPath).select('NAME, 'ITEM_SZ, 'ITEM).distinct // 아이템 태그 매핑 테이블
    val baseDf  = fileFuncCls.rParquet(filePathCls.basePath) // 분석월 주류 유흥 정제 데이터
    val comMaster = fileFuncCls.rParquet(filePathCls.comMainPath)

    val baseCol = Seq("ISS_ID", "ITEM_NO", "SUP_RGNO", "SUP_NM", "SUP_ADDR", "BYR_RGNO", "BYR_NM", "BYR_ADDR", "ITEM_NM", "NAME", "ITEM", "ITEM_SZ",
      "ITEM_QT", "ITEM_UP", "SUP_AMT", "BARCODE", "CHANNEL", "TYPE", "TAX_AMT", "WR_DT", "DATE").map(col) // [2020.10.05] AG

    val yeongNamCham = List("참저도주미분류", "참17미분류") // 영남권 해당 아이템
    val suDoCham = List("참오리지널미분류", "참이슬미분류") // 수도권 해당 아이템

    val unCateCham = (yeongNamCham ::: suDoCham).toDF("ITEM").withColumn("EXIST", lit("Y")) // 변경할 아이템
    val (unCateChamComDf, chamComDf) = reMatchingCom(comMaster)

    val baseItemDf = baseDf.join(cumulDf, Seq("NAME", "ITEM_SZ"), "left"). // 아이템 태그 부착
      join(unCateCham, Seq("ITEM"), "left") // 변경할 아이템 부착

    val baseNoChgDF = baseItemDf.filter('EXIST.isNull).select(baseCol:_*) // 변동 사항 없는 데이터
    val (baseUnCateChamDF, baseChamDF) = chngData(baseItemDf, unCateChamComDf, chamComDf)
    val itemRenmUdf = itemChangeUdf()
    val baseFinalDf = baseNoChgDF.union(baseUnCateChamDF).union(baseChamDF).
      withColumn("ITEM", itemRenmUdf('SUP_RGNO, 'NAME, 'ITEM_SZ, 'ITEM)).
      select(baseCol:_*)

    // [2022.01.27] 검증값 추가

    baseFinalDf.filter($"ITEM".contains("클라우드") && $"SUP_RGNO" === "2298123864").select("NAME", "ITEM_SZ", "ITEM").distinct().show(100, false)

    if (miniNumTest(baseDf, baseFinalDf) == false || miniTest(baseFinalDf, unCateCham) == false) return
    baseFinalDf.repartition(1).write.parquet(filePathCls.baseTaggedPath)
  }
}
