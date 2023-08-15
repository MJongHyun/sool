/**
 * dk 집계 시 필요한 데이터 로드

 */
package sool.service.aggregate.agg_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath

import java.time._
import java.time.format.DateTimeFormatter

class GetAggDfsDk(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  // 필요 클래스 선언
  val jdbcGetCls = new JdbcGet(spark)
  val getTimeCls = new GetTime()
  val fileFuncCls = new FileFunc(spark)

  // DK 메뉴 및 필터 테이블 로드
  def getDkMnFltrDfs(ethDt: String) = {
    val dkMnIntr = jdbcGetCls.getMnIntrTbl("DK_MN_INTR", ethDt)
    val dkFltrIntr = jdbcGetCls.getFltrIntrTbl("DK_FLTR_INTR")
    val dkFltrRl = jdbcGetCls.getFltrRLTbl("DK_FLTR_RL")
    (dkMnIntr, dkFltrIntr, dkFltrRl)
  }

  // DK 마트 로드
  def getMartDk(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val martDkb = fileFuncCls.rParquet(filePathCls.dkwMartBPath)
    val martDkw = fileFuncCls.rParquet(filePathCls.dkwMartWPath)
    (martDkb, martDkw)
  }

  // 필터 개요와 조인한 필터 ID별 필터 규칙 테이블 생성
  def getDkFltrQry(dkFltrRl: DataFrame, dkFltrIntr: DataFrame) = {
    // CND_VAL 에 따옴표 붙이기, DQ : Double Quote
    val dkFltrRlAddDqCol = dkFltrRl.
      withColumn("CND_VAL_SQ", concat(lit("\""), 'CND_VAL, lit("\"")))

    // CND_VAL 하나로 합치기
    val dkFltrRlClctVal = dkFltrRlAddDqCol.groupBy('FLTR_ID, 'ITEM_COL_CD, 'CND_OPR).
      agg(concat_ws(", ", collect_list("CND_VAL_SQ")).as("CND_VAL_C"))

    // "in" 혹은 "not in" 에 해당하는 CND_VAL 은 괄호로 묶어주기
    val dkFltrRlGModValC = dkFltrRlClctVal.withColumn("CND_VAL_C",
      when('CND_OPR === "in" or 'CND_OPR === "not in",
        concat(lit("("), 'CND_VAL_C, lit(")"))).otherwise('CND_VAL_C))

    // 각각 개별 쿼리문 생성
    val dkFltrRlAddQryCol = dkFltrRlGModValC.withColumn("QRY",
      concat('ITEM_COL_CD, lit(" "), 'CND_OPR, lit(" "), 'CND_VAL_C))

    // 필터 아이디별 쿼리문들 묶어주기
    val dkFltrRlQry = dkFltrRlAddQryCol.groupBy('FLTR_ID).
      agg(concat_ws(" AND ", collect_list("QRY")).as("QRY_C"))

    val dkFltrQry = dkFltrIntr.join(dkFltrRlQry, Seq("FLTR_ID")) // 필터 개요 테이블이랑 조인하기
    dkFltrQry
  }


  // 저번 달 분석 결과 로드
  def getMenuAnlysResDfBf1m(ethDt: String, flag: String) = {
    val ethBf1m = getTimeCls.getEthBf1m(ethDt) // 저번 달
    val filePathCls = new FilePath(ethBf1m, flag) // input 값으로 저번 달 값을 받는다.
    val menuAnlysResDfBf1m = spark.read.parquet(filePathCls.dkMenuAnlysResDfPath)
    menuAnlysResDfBf1m
  }

  // 저번 달 맥주, 위스키 히스토리 로드
  def getHstryDkbwBf1m(ethDt: String, flag: String) = {
    val ethBf1m = getTimeCls.getEthBf1m(ethDt) // 저번 달
    val filePathCls = new FilePath(ethBf1m, flag) // input 값으로 저번 달 값을 받는다.
    val hstryDkbBf1m = fileFuncCls.rParquet(filePathCls.dkHstryBeerPath)
    val hstryDkwBf1m = fileFuncCls.rParquet(filePathCls.dkHstryWhiskyPath)
    (hstryDkbBf1m, hstryDkwBf1m)
  }

  // 이번 달 맥주, 위스키 히스토리 로드 후 필요한 데이터만 추출
  def getHstryDkbw(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag) // input 값으로 저번 달 값을 받는다.
    val hstryDkbRaw = fileFuncCls.rParquet(filePathCls.dkHstryBeerPath)
    val hstryDkwRaw = fileFuncCls.rParquet(filePathCls.dkHstryWhiskyPath)

    // DK 회계연도 기준으로 현재 집계 연도만 추출하기
    val fltrDkDt = udf((ym: String, ethDt: String) => {
      val dtFrmt = DateTimeFormatter.ofPattern("yyyyMMdd")

      // ym to Dk 회계연도 기준
      val ymParse = LocalDate.parse(ym + "01", dtFrmt)
      val ymDk = ymParse.plusMonths(6)
      val ymDkYear = ymDk.getYear

      // ethDt to Dk 회계연도 기준
      val ethDtParse = LocalDate.parse(ethDt + "01", dtFrmt)
      val ethDtDk = ethDtParse.plusMonths(6)
      val ethDtDkYear = ethDtDk.getYear

      if (ymDkYear == ethDtDkYear) true
      else false
    })

    // 집계연월을 기준으로 DK 회계연도의 해당 연도만 추출
    val hstryDkbFltr = hstryDkbRaw.filter(fltrDkDt('YM, lit(ethDt)))
    val hstryDkwFltr = hstryDkwRaw.filter(fltrDkDt('YM, lit(ethDt)))

    // 히스토리 전체 메뉴 아이디, 주소만 추출
    val mnIdAddrSlctCols = Seq("MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDR3")
    val hstryDkbMnIdAddr = hstryDkbRaw.select(mnIdAddrSlctCols.map(col): _*).distinct // distinct 필수!
    val hstryDkwMnIdAddr = hstryDkwRaw.select(mnIdAddrSlctCols.map(col): _*).distinct // distinct 필수!

    // 히스토리 전체 메뉴 아이디, 주소 테이블에 현재 DK 회계연도 데이터 테이블 조인
    // ---> 현재 DK 회계연도 데이터 + 과거 메뉴아이디 & 주소 데이터(값은 null)
    val hstryDkb = hstryDkbMnIdAddr.join(hstryDkbFltr, mnIdAddrSlctCols, "left_outer")
    val hstryDkw = hstryDkwMnIdAddr.join(hstryDkwFltr, mnIdAddrSlctCols, "left_outer")
    (hstryDkb, hstryDkw)
  }

  // 이번 달 집계 결과를 맥주, 위스키로 분류하여 로드
  def getMenuAnlysResDf(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val menuAnlysResDf = fileFuncCls.rParquet(filePathCls.dkMenuAnlysResDfPath)
    val resDkb = menuAnlysResDf.filter('MENU_ID.startsWith("B")) // beer
    val resDkw = menuAnlysResDf.filter('MENU_ID.startsWith("W")) // whisky
    (resDkb, resDkw)
  }

  /* DB 변경에 따라 결과 저장 필요 없음 (추후 삭제 예정)*/
  // DK 최종 집계 결과 데이터 로드
  def getDkViewResDfs(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val dkViewResWeb = fileFuncCls.rParquet(filePathCls.dkViewResWebPath)
    val dkViewResExcel = fileFuncCls.rParquet(filePathCls.dkViewResExcelPath)
    (dkViewResWeb, dkViewResExcel)
  }
}

