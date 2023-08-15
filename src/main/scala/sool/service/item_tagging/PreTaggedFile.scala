package sool.service.item_tagging

import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, when, _}
import sool.common.path.FilePath

class PreTaggedFile(spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)

  // item_info_FINAL.parquet 데이터 개수와 item_info_BEFORE.parquet 데이터 개수 비교
  def checkItemInfoFnl(generatedDf: DataFrame, itemInfoFinal: DataFrame) = {
    val checkBoolean = generatedDf.count == itemInfoFinal.count
    if (checkBoolean == false) {
      println(s"${getTimeCls.getTimeStamp()}, " +
        s"item_info_FINAL.parquet 데이터 내에 item_info_BEFORE.parquet 데이터 개수가 다릅니다. 문제가 있으니 확인하십시오.")
    }
    checkBoolean
  }

  // 아이템 모집단 마스터에 존재하지 않은 아이템을 태깅한 경우 확인
  def checkFnlNminfoDfItem(fnlNminfoDf: DataFrame, dbItem: DataFrame) = {
    val fnlNminfoDfItem = fnlNminfoDf.select('ITEM).distinct
    val checkItem = fnlNminfoDfItem.except(dbItem.select("ITEM"))
    val checkItemCnt = checkItem.count

    if (checkItemCnt > 0) {
      println(s"${getTimeCls.getTimeStamp()}, final_nminfo_df 데이터 중 아이템 모집단 마스터에 없는 아이템이 존재합니다. 확인하십시오.")
      println(s"${getTimeCls.getTimeStamp()}, 개수: ${checkItemCnt}")
      checkItem.show(checkItemCnt.toInt, false)
    } else {
      println(s"${getTimeCls.getTimeStamp()}, final_nminfo_df 데이터 중 아이템 모집단 마스터에 없는 아이템이 존재하지 않습니다. 이상 없습니다.")
    }
    checkItemCnt==0
  }

  // 아이템 태깅 결과값이 없는 경우 확인
  def checkFnlNminfoDf(fnlNminfoDf: DataFrame, base: DataFrame) = {
    val fnlNminfoDfSlct = fnlNminfoDf.select('NAME, 'ITEM_SZ, 'ITEM)
    val baseFnlNminfo = base.join(fnlNminfoDfSlct, Seq("NAME", "ITEM_SZ"), "left") // base 에 태깅된 ITEM 붙이기
    val checkBaseFnlNminfo = baseFnlNminfo.filter('ITEM.isNull).select('NAME, 'ITEM_SZ).distinct
    val checkBaseFnlNminfoCnt = checkBaseFnlNminfo.count

    if (checkBaseFnlNminfoCnt > 0) {
      println(s"${getTimeCls.getTimeStamp()}, base 에 final_nminfo_df 를 조인했을 때 아이템 태깅 결과값이 null 인 경우가 존재합니다. 확인하십시오.")
      println(s"${getTimeCls.getTimeStamp()}, 개수: ${checkBaseFnlNminfoCnt}")
      checkBaseFnlNminfo.show(checkBaseFnlNminfoCnt.toInt, false)
    } else {
      println(s"${getTimeCls.getTimeStamp()}, base 에 final_nminfo_df 를 조인했을 때 아이템 태깅 결과값이 null 인 경우가 없습니다. 이상 없습니다.")
    }
    checkBaseFnlNminfoCnt==0
  }

  // 메인
  def runPreTagged(ethDt: String, flag: String): String = {
    val filePathCls = new FilePath(ethDt, flag)

    val generatedDf = fileFuncCls.rParquet(filePathCls.generatedDfPath) // 태깅 완료전 generatedDF.parquet 로드
    val itemInfoFnlCsv = fileFuncCls.rCsv(filePathCls.itemInfoFnlCsvPath, "true") // 수동태깅 완료된 item_info_FINAL.csv 로드

    // item_info_FINAL.parquet 데이터 개수와 item_info_BEFORE.parquet 데이터 개수 비교
    if (checkItemInfoFnl(generatedDf, itemInfoFnlCsv) == false) return ("fail")

    fileFuncCls.wParquet(itemInfoFnlCsv, filePathCls.itemInfoFnlPath) // item_info_FINAL ---> item_info_FINAL.parquet 로 저장

    val nminfo = fileFuncCls.rParquet(filePathCls.itemInfoFnlPath).
      withColumn("SAW", when($"SAW".isNull, lit("봄")).otherwise($"SAW")).
      withColumn("TYPE", when(length($"TYPE") === 1, concat(lit("0"), $"TYPE")).otherwise($"TYPE")).
      withColumn("ITEM_SZ", 'ITEM_SZ + ".0").
      na.fill("")

    // 아이템 태깅 검증 및 이번 base에 나온 NAME, ITEM_SZ 값 다 나왔는지 확인
    val mainDF = fileFuncCls.rParquet(filePathCls.ItemPath)
    // 아이템 모집단 마스터에 존재하지 않은 아이템을 적은 경우 확인
    if (checkFnlNminfoDfItem(nminfo, mainDF) == false) return ("fail")

    // 아이템 매칭 결과값이 없는 경우 확인
    val baseDF = fileFuncCls.rParquet(filePathCls.basePath)
    if (checkFnlNminfoDf(nminfo, baseDF) == false) return ("fail")

    fileFuncCls.wParquet(nminfo, filePathCls.fnlNminfoDfPath)
    ("success")
  }
}
