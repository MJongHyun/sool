package sool.service.item_tagging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath

class UpdateCumulDict(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._
  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)

  // 누적 품목사전 구하기
  def getCumulNminfoDf(bfDictDf: DataFrame, fnlNminfoDf: DataFrame) = {
    // 컬럼 순서
    val colsOrder1 = Seq("NAME", "ITEM_SZ").map(col)
    val colsOrder2 = Seq("WR_DT", "BARCODE", "TYPE", "NAME", "ITEM_SZ", "ITEM_UP", "AMT", "ITEM", "SAW").map(col)

    /* 이번 달 아이템 태깅된 품목사전(final_nminfo_df) 에 등장하지 않은 전월 누적사전(beforeDictDF) 의 품목, 사이즈 관련 데이터 추출 */
    val bfDictDfSlct = bfDictDf.select(colsOrder1:_*).distinct  // 전월 누적사전 품목, 사이즈만 추출
    val fnlNminfoDfSlct = fnlNminfoDf.select(colsOrder1:_*).distinct  // 이번 달 아이템 태깅된 품목사전
    val notAprInFnlNminfo = bfDictDfSlct.except(fnlNminfoDfSlct)  // 아이템 태깅된 품목사전에 등장하지 않은 전월 누적사전 데이터
    val onlyBfDictDf = bfDictDf.
      join(notAprInFnlNminfo, Seq("NAME", "ITEM_SZ"), "inner").
      select(colsOrder2:_*)

    /* 누적 품목사전 구하기 */
    val cumulNminfoDf = fnlNminfoDf.select(colsOrder2:_*).union(onlyBfDictDf).distinct()
    cumulNminfoDf
  }

  // cumul_nminfo_df.parquet 생성 전 데이터 내 NAME, ITEM_SZ 중복 체크
  def checkCumulNminfoDf(cumulNminfoDf: DataFrame) = {
    val checkBoolean = cumulNminfoDf.groupBy("NAME", "ITEM_SZ").count.filter('count > 1).count == 0
    if (checkBoolean == false) {
      println(s"${getTimeCls.getTimeStamp()}, cumul_nminfo_df.parquet 생성 전 데이터 내 NAME, ITEM_SZ 중복이 존재합니다. 확인하십시오.")
    }
    checkBoolean
  }

  // cumul_nminfo_df.parquet 관련 info 정보
  def showInfoAboutCumulNminfoDf(cumulBf: DataFrame, cumulAf: DataFrame, ethDtBf1m: String, ethDt: String) =  {
    val ckBF = cumulBf.groupBy("SAW").count.toDF("SAW", ethDtBf1m)
    val ckAF = cumulAf.groupBy("SAW").count.toDF("SAW", ethDt)
    val ckDF = ckBF.join(ckAF, Seq("SAW"), "left").withColumn("GAP", col(ethDt) - col(ethDtBf1m))
    println("========================================")
    println("전달 누적사전 수 : " + cumulBf.count)
    println("이번달 누적사전 수 : " + cumulAf.count)
    println("추가 레코드 수 : " + (cumulAf.count.toInt - cumulBf.count.toInt))
    println("========================================")
    ckDF.show(false)
  }

  // 메인
  def runUpdateCumulDict(ethDt: String, flag: String): String = {
    val ethDtBf = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethDtBf, flag)
    val ethDtAf = getTimeCls.getEthAf1m(ethDt)   // 집계연월의 다음 달
    val filePathCls = new FilePath(ethDt, flag) // 집계연월

    val cumulBf = fileFuncCls.rParquet(filePathClsBf1m.cumulNminfoDfNewPath)
    val nowDict = fileFuncCls.rParquet(filePathCls.fnlNminfoDfPath)

    /* 누적 품목사전 업데이트 */
    val cumulNminfoDf = getCumulNminfoDf(cumulBf, nowDict)
    if (checkCumulNminfoDf(cumulNminfoDf) == false) return ("fail")
    fileFuncCls.wParquet(cumulNminfoDf, filePathCls.cumulNminfoDfPath) // 누적 품목사전 업데이트(저장)


    val cumulAf = fileFuncCls.rParquet(filePathCls.cumulNminfoDfPath)
    showInfoAboutCumulNminfoDf(cumulBf, cumulAf, ethDtBf, ethDt)  // 관련 정보 확인
    ("success")
  }
}
