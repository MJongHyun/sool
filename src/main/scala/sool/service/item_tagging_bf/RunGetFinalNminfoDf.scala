/**
 * 아이템 태깅 완료된 아이템 품목 사전(item_info_FINAL.parquet, final_nminfo_df.parquet) 생성

 */
package sool.service.item_tagging_bf

import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, when}
import sool.common.path.FilePath

class RunGetFinalNminfoDf(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)

  // item_info_FINAL.parquet 데이터 개수와 item_info_BEFORE.parquet 데이터 개수 비교
  def checkItemInfoFnl(itemInfoFnl: DataFrame, itemInfoBf: DataFrame) = {
    val checkItemInfoFnl = itemInfoFnl.
      withColumn("SAW", when($"SAW".isNull, lit("봄")).otherwise($"SAW")).
      withColumn("ITEM_SZ", 'ITEM_SZ + ".0")
    val checkBoolean = checkItemInfoFnl.count == itemInfoBf.count
    if (checkBoolean == false) {
      println(s"${getTimeCls.getTimeStamp()}, " +
        s"item_info_FINAL.parquet 데이터 내에 item_info_BEFORE.parquet 데이터 개수가 다릅니다. 문제가 있으니 확인하십시오.")
    }
    checkBoolean
  }

  // DB 각 고객사별 ITEM 및 기타 ITEM 추출
  def getDbItem() = {
    val hjBrItem = jdbcGetCls.getItemTbl("HJ_BR_ITEM").select('BR_NM.as("ITEM"))
    val hjSjItem = jdbcGetCls.getItemTbl("HJ_SJ_ITEM").select('SJ_NM.as("ITEM"))
    val dkBrItem = jdbcGetCls.getItemTbl("DK_BR_ITEM").select('BR_NM.as("ITEM"))
    val dkWskItem = jdbcGetCls.getItemTbl("DK_WSK_ITEM").select('WSK_NM.as("ITEM"))
    val hkAgrnItem = jdbcGetCls.getItemTbl("HK_AGRN_ITEM").select('ITEM_NM.as("ITEM"))
    val nonPopItem = Seq(
      "비모집단기타주류", "비모집단기타주류일본술", "비모집단기타주류중국술", "비모집단맥주병", "비모집단맥주캔",
      "비모집단맥주케그", "비모집단맥주페트", "비모집단소주기타용기", "비모집단소주병", "비모집단소주팩",
      "비모집단소주페트", "비모집단소주포켓", "비모집단와인", "비모집단위스키", "비모집단탁주"
    ).toDF("ITEM")
    val unClsItem = Seq("비주류", "참이슬미분류", "참17미분류", "참오리지널미분류", "참저도주미분류").toDF("ITEM")
    val dbItemDfs = Seq(hjBrItem, hjSjItem, dkBrItem, dkWskItem, hkAgrnItem, nonPopItem, unClsItem)
    val dbItem = dbItemDfs.reduce(_ union _).distinct
    dbItem
  }

  // 아이템 모집단 마스터에 존재하지 않은 아이템을 태깅한 경우 확인
  def checkFnlNminfoDfItem(fnlNminfoDf: DataFrame, dbItem: DataFrame) = {
    val fnlNminfoDfItem = fnlNminfoDf.select('ITEM).distinct
    val checkItem = fnlNminfoDfItem.except(dbItem)
    val checkItemCnt = checkItem.count

    if (checkItemCnt > 0) {
      println(s"${getTimeCls.getTimeStamp()}, final_nminfo_df 데이터 중 아이템 모집단 마스터에 없는 아이템이 존재합니다. 확인하십시오.")
      println(s"${getTimeCls.getTimeStamp()}, 개수: ${checkItemCnt}")
      checkItem.show(checkItemCnt.toInt, false)
    } else {
      println(s"${getTimeCls.getTimeStamp()}, final_nminfo_df 데이터 중 아이템 모집단 마스터에 없는 아이템이 존재하지 않습니다. 이상 없습니다.")
    }
  }

  // 아이템 태깅 결과값이 없는 경우 확인
  def checkFnlNminfoDf(fnlNminfoDf: DataFrame, base: DataFrame) = {
    val fnlNminfoDfSlct = fnlNminfoDf.select('NAME, 'ITEM_SZ, 'ITEM)
    val baseFnlNminfo = base.join(fnlNminfoDfSlct, Seq("NAME", "ITEM_SZ"), "left")  // base 에 태깅된 ITEM 붙이기
    val checkBaseFnlNminfo = baseFnlNminfo.filter('ITEM.isNull).select('NAME, 'ITEM_SZ).distinct
    val checkBaseFnlNminfoCnt = checkBaseFnlNminfo.count

    if (checkBaseFnlNminfoCnt > 0) {
      println(s"${getTimeCls.getTimeStamp()}, base 에 final_nminfo_df 를 조인했을 때 아이템 태깅 결과값이 null 인 경우가 존재합니다. 확인하십시오.")
      println(s"${getTimeCls.getTimeStamp()}, 개수: ${checkBaseFnlNminfoCnt}")
      checkBaseFnlNminfo.show(checkBaseFnlNminfoCnt.toInt, false)
    } else {
      println(s"${getTimeCls.getTimeStamp()}, base 에 final_nminfo_df 를 조인했을 때 아이템 태깅 결과값이 null 인 경우가 없습니다. 이상 없습니다.")
    }
  }

  // 메인
  def runGetItemInfoFinal(ethDt: String, flag: String): Unit = {
    val filePathCls = new FilePath(ethDt, flag)

    // item_info_FINAL.parquet 생성
    val itemInfoFnlCsv = fileFuncCls.rCsv(filePathCls.itemInfoFnlCsvPath, "true")  // 태깅 완료된 item_info_FINAL.csv 로드
    fileFuncCls.wParquet(itemInfoFnlCsv, filePathCls.itemInfoFnlPath)  // item_info_FINAL.parquet 으로 저장

    // item_info_FINAL.parquet 로드
    val itemInfoBf = spark.read.parquet(filePathCls.itemInfoBfPath) // itemInfoFnl 이랑 비교하기 위해 로드
    val itemInfoFnl = spark.read.parquet(filePathCls.itemInfoFnlPath)

    // item_info_FINAL.parquet 데이터 개수와 item_info_BEFORE.parquet 데이터 개수 비교
    if (checkItemInfoFnl(itemInfoFnl, itemInfoBf) == false) return
    fileFuncCls.wParquet(itemInfoFnl, filePathCls.fnlNminfoDfPath)  // item_info_FINAL ---> final_nminfo_df 로 저장

    /* check */
    val fnlNminfoDf = spark.read.parquet(filePathCls.fnlNminfoDfPath)
    val dbItem = getDbItem()  // DB 각 고객사별 ITEM 및 기타 ITEM 추출
    val base = spark.read.parquet(filePathCls.basePath)
    checkFnlNminfoDfItem(fnlNminfoDf, dbItem) // 아이템 모집단 마스터에 존재하지 않은 아이템을 태깅한 경우 확인
    checkFnlNminfoDf(fnlNminfoDf, base)
  }
}
