/**
 * 최초 품목 사전(item_info_BEFORE.parquet) 생성

 */
package sool.service.item_tagging_bf

import sool.common.function.{FileFunc, GetTime}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, dense_rank, lit, rank, udf, when}
import org.apache.spark.sql.expressions.Window
import sool.common.path.FilePath

class RunGetItemInfoBefore(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()

  // tagged 파일 내 NAME, ITEM_SZ 중복 여부
  def checkDuplicateInTagged(tagged: DataFrame) = {
    val checkBoolean = tagged.count == tagged.select("NAME", "ITEM_SZ").distinct.count

    if (checkBoolean) {
      println(s"${getTimeCls.getTimeStamp()}, tagged 파일 내 NAME, ITEM_SZ 의 중복이 존재하지 않습니다.")
    } else {
      println(s"${getTimeCls.getTimeStamp()}, tagged 파일 내 NAME, ITEM_SZ 의 중복이 존재합니다. 실행을 종료합니다.")
    }
    checkBoolean
  }

  // 태깅대상(generatedDF) 중 태깅되지 않은게 있는지 체크 (있으면 안됨!)
  def checkTagged(tagged: DataFrame, generatedDf: DataFrame) = {
    val generatedDfSlct = generatedDf.select('NAME, 'ITEM_SZ).distinct
    val taggedSlct = tagged.select('NAME, 'ITEM_SZ)
    val checkBoolean = generatedDfSlct.except(taggedSlct).count == 0

    if (checkBoolean) {
      println(s"${getTimeCls.getTimeStamp()}, 태깅 대상(generatedDF) 중에 태깅되지 않은 것 없이 모두 태깅 되었습니다.")
    } else {
      println(s"${getTimeCls.getTimeStamp()}, 태깅 대상(generatedDF) 중 태깅되지 않은 것이 존재합니다. 실행을 종료합니다.")
    }
    checkBoolean
  }

  // [분석월 품목사전-머신] 생성 1단계: 기존표현에 태그 붙이기(from 전월 품목사전) & 신규표현 예측된 것 합치기
  def getCurrentDict(cumulNminfoDfBf1m: DataFrame, existedDf: DataFrame, tagged: DataFrame) = {
    val double2String = udf((a: Double) => a.toString)
    val dictCols = Seq("NAME", "ITEM_SZ", "AMT", "ITEM", "OLD_NEW").map(col)

    // 기존표현에 (전월 품목사전) 태그 붙이기 & 자료형 변환
    val cumulNminfoDfBf1mSlct = cumulNminfoDfBf1m.select('NAME, 'ITEM_SZ, 'ITEM)
    val currentDictExistedPre = existedDf.join(cumulNminfoDfBf1mSlct, Seq("NAME", "ITEM_SZ")) // 여태까지 태깅된 ITEM 붙이기
    val currentDictExisted = currentDictExistedPre.
      withColumn("OLD_NEW", lit("OLD")).  // 기존 품목사전에 있던 것들이니 OLD
      withColumn("ITEM_SZ", double2String('ITEM_SZ.cast("Double"))).
      select(dictCols:_*)

    // 신규표현 예측된 것의 자료형 변환
    val currentDictTagged = tagged.
      withColumn("OLD_NEW", lit("NEW")).  // 기존 품목사전에 없던 것들이니 NEW
      withColumn("ITEM_SZ", double2String('ITEM_SZ)).
      withColumn("AMT", 'AMT.cast("Double")).
      select(dictCols:_*)

    // union
    val currentDict = currentDictExisted.union(currentDictTagged).distinct
    currentDict
  }

  // [분석월 품목사전-머신] 생성 2단계: 품목사전에 부착할 [품목명, 용량]의 '대표 바코드 선정'
  def getRprsnBrcd(base: DataFrame) = {
    val rprsnBrcdPre = base.groupBy("NAME", "ITEM_SZ", "BARCODE").count
    val w = Window.partitionBy('NAME, 'ITEM_SZ).orderBy('count.desc, 'BARCODE.asc)
    val rprsnBrcd = rprsnBrcdPre.
      withColumn("RANK", dense_rank().over(w)).
      filter('RANK === 1).
      drop("RANK", "count").
      na.fill("", Seq("BARCODE"))
    rprsnBrcd
  }

  // [분석월 품목사전-머신] 생성 3단계: 품목사전에 바코드 부착 (파일명: item_info_BEFORE.parquet)
  def getItemInfoBf(currentDict: DataFrame, rprsnBrcd: DataFrame, base: DataFrame, cumulNminfoDfBf1m: DataFrame) = {
    val currentDictBrcdPre = currentDict.join(rprsnBrcd, Seq("NAME", "ITEM_SZ"))
    val currentDictBrcd = currentDictBrcdPre.select("NAME", "ITEM_SZ", "BARCODE", "ITEM", "AMT", "OLD_NEW")

    // MOST_ITEM 추출
    val mostItemDfPre = currentDictBrcd.groupBy('BARCODE, 'ITEM.as("MOST_ITEM")).count
    val w = Window.partitionBy('BARCODE).orderBy('count.desc, 'MOST_ITEM)
    val mostItemDf = mostItemDfPre.
      withColumn("RANK", rank().over(w)).
      filter('RANK === 1).
      drop("RANK", "count")

    // TYPE 추출
    val typeDfPre = base.select('NAME, 'ITEM_SZ, 'TYPE).distinct
    val typeDf = typeDfPre.
      groupBy("NAME", "ITEM_SZ").
      agg(concat_ws(" ", collect_list($"TYPE")).as("TYPE"))

    // 바코드의 대표 태그 부착
    val itemInfoBfPre1 = currentDictBrcd.join(mostItemDf, Seq("BARCODE"), "left")
    val itemInfoBfPre2 = itemInfoBfPre1.
      withColumn("MINOR_MAJOR", when('ITEM =!= 'MOST_ITEM, "minor").otherwise("major"))

    // 봄, 안봄 부착 / 신규 표현 -- 안봄 부착 [2019.07.12 추가]
    val cumulNminfoDfBf1mSlct = cumulNminfoDfBf1m.select("NAME","ITEM_SZ", "SAW")
    val itemInfoBfPre3 = itemInfoBfPre2.join(cumulNminfoDfBf1mSlct, Seq("NAME", "ITEM_SZ"), "left")
    val itemInfoBfPre4 = itemInfoBfPre3.
      withColumn("SAW", when($"OLD_NEW" === "NEW", "안봄").otherwise($"SAW"))

    // 주종 부착 -- [2019.09.16 추가]
    val itemInfoBfPre5 = itemInfoBfPre4.join(typeDf, Seq("NAME", "ITEM_SZ"), "left")

    // 최종
    val itemInfoBf = itemInfoBfPre5.select(
      "BARCODE", "TYPE", "NAME",
      "ITEM_SZ", "ITEM", "MOST_ITEM",
      "AMT", "OLD_NEW", "SAW"
    )
    itemInfoBf
  }

  // itemInfoBf 최종 점검
  def checkItemInfoBf(itemInfoBf: DataFrame) = {
    val checkBooleanPre1 = itemInfoBf.groupBy("NAME", "ITEM_SZ").count
    val checkBooleanPre2 = checkBooleanPre1.
      filter('count > 1 || 'NAME.isNull || 'NAME === "" || 'ITEM_SZ.isNull || 'ITEM_SZ === "")
    val checkBoolean = checkBooleanPre2.count == 0
    if (checkBoolean == false) {
      println(s"${getTimeCls.getTimeStamp()}, itemInfoBf 데이터에 문제가 있습니다. item_info_BEFORE.parquet 을 생성하지 않겠습니다.")
    }
    checkBoolean
  }

  // 메인
  def runGetItemInfoBefore(ethDt: String, flag: String): Unit = {
    val ethBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)

    /* 필요 데이터 준비 */
    val base = spark.read.parquet(filePathCls.basePath)
    val tagged = spark.read.parquet(filePathCls.taggedDfPath)
    val existedDf = spark.read.parquet(filePathCls.existedDfPath)
    val generatedDf = spark.read.parquet(filePathCls.generatedDfPath)

    // tagged 파일 내 NAME, ITEM_SZ 중복 여부 체크 / 태깅대상(generatedDF) 중 태깅되지 않은게 있는지 체크(있으면 안됨!)
    if (checkDuplicateInTagged(tagged) == false || checkTagged(tagged, generatedDf) == false) return

    // 누적사전 로드
    val cumulNminfoDfBf1m = if (fileFuncCls.checkS3FileExist(filePathClsBf1m.cumulNminfoDfNewPath)) {
      spark.read.parquet(filePathClsBf1m.cumulNminfoDfNewPath)  // 누적사전 변경으로 인해 경로 변경 시 사용
    } else {
      spark.read.parquet(filePathClsBf1m.cumulNminfoDfPath)
    }

    // [분석월 품목사전-머신] 생성 1단계: 기존표현에 태그 붙이기(from 전월 품목사전) & 신규표현 예측된 것 합치기
    val currentDict = getCurrentDict(cumulNminfoDfBf1m, existedDf, tagged)

    // [분석월 품목사전-머신] 생성 2단계: 품목사전에 부착할 [품목명, 용량]의 '대표 바코드 선정'
    val rprsnBrcd = getRprsnBrcd(base)

    // [분석월 품목사전-머신] 생성 3단계: 품목사전에 바코드, 타입, MOST_ITEM 부착 (파일명: item_info_BEFORE.parquet)
    val itemInfoBf = getItemInfoBf(currentDict, rprsnBrcd, base, cumulNminfoDfBf1m)

    if (checkItemInfoBf(itemInfoBf) == false) return  // itemInfoBf 점검
    fileFuncCls.wParquet(itemInfoBf, filePathCls.itemInfoBfPath)  // 최종 저장
  }
}