/**

 */
package sool.service.aggregate.agg_hj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, concat, concat_ws, lit, trim, when}
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath

class GetAggDfsHj(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  val jdbcGetCls = new JdbcGet(spark)


  // HJ 아이템 테이블 로드
  def getHjItemTbl() = {
    // DB, HJ 맥주, 소주 아이템 마스터
    val hjbItem = jdbcGetCls.getItemTbl("HJ_BR_ITEM")
    val hjsItem = jdbcGetCls.getItemTbl("HJ_SJ_ITEM")
    (hjbItem, hjsItem)
  }

  // HJ 마트 로드
  def getMartHj(ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val martHjb = spark.read.parquet(filePathCls.hjwMartBPath)
    val martHjs = spark.read.parquet(filePathCls.hjwMartSPath)
    val martHjbNonAddr = spark.read.parquet(filePathCls.hjwMartBExceptAddrPath)
    val martHjsNonAddr = spark.read.parquet(filePathCls.hjwMartSExceptAddrPath)
    (martHjb, martHjs, martHjbNonAddr, martHjsNonAddr)
  }

  // HJ 메뉴 및 필터 테이블 로드
  def getHjMnFltrDfs(ethDt:String) = {
    val hjMnIntr = jdbcGetCls.getMnIntrTbl("HJ_MN_INTR", ethDt)
    val hjFltrIntr = jdbcGetCls.getFltrIntrTbl("HJ_FLTR_INTR")
    val hjFltrRl = jdbcGetCls.getFltrRLTbl("HJ_FLTR_RL")
    (hjMnIntr, hjFltrIntr, hjFltrRl)
  }

  // HJ 집계에 필요한 데이터들 로드
  def getDfsHj(ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    // com_main_dk
    val comMain = spark.read.parquet(filePathCls.comMasterDkPath) // 주소마스터 경로 변경 [2021.03.10]

    // lv2w (ex: 동서울, 진주, 대전, 울산, 성남, 포항, 수원 등)
    val lv2w = spark.read.parquet(filePathCls.hjwLv2Path)

    // ANVR13 집계 필터 기준
    val supCdDimension = spark.read.parquet(filePathCls.hjSupCdDimensionPath)

    // CD_ANVR parquet
    val cdAnvrParquet = spark.read.parquet(filePathCls.hjCdAnvrParquetPath)
    (comMain, lv2w, supCdDimension, cdAnvrParquet)
  }

  // 필터 개요와 조인한 필터 ID별 필터 규칙 테이블 생성
  def getHjFltrQry(hjFltrRl:DataFrame, hjFltrIntr:DataFrame) = {
    // CND_VAL 에 따옴표 붙이기, DQ : Double Quote
    val hjFltrRlAddDqCol = hjFltrRl.
      withColumn("CND_VAL_SQ", concat(lit("\""), 'CND_VAL, lit("\"")))

    // CND_VAL 하나로 합치기
    val hjFltrRlClctVal = hjFltrRlAddDqCol.groupBy('FLTR_ID, 'ITEM_COL_CD, 'CND_OPR).
      agg(concat_ws(", ", collect_list("CND_VAL_SQ")).as("CND_VAL_C"))

    // "in" 혹은 "not in" 에 해당하는 CND_VAL 은 괄호로 묶어주기
    val hjFltrRlGModValC = hjFltrRlClctVal.withColumn("CND_VAL_C",
      when('CND_OPR === "in" or 'CND_OPR === "not in",
        concat(lit("("), 'CND_VAL_C, lit(")"))).otherwise('CND_VAL_C))

    // 각각 개별 쿼리문 생성
    val hjFltrRlAddQryCol = hjFltrRlGModValC.withColumn("QRY",
      concat('ITEM_COL_CD, lit(" "), 'CND_OPR, lit(" "), 'CND_VAL_C))

    // 필터 아이디별 쿼리문들 묶어주기
    val hjFltrRlQry = hjFltrRlAddQryCol.groupBy('FLTR_ID).
      agg(concat_ws(" AND ", collect_list("QRY")).as("QRY_C"))

    val hjFltrQry = hjFltrIntr.join(hjFltrRlQry, Seq("FLTR_ID"))  // 필터 개요 테이블이랑 조인하기
    hjFltrQry
  }
}
