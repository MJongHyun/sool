/**
 * get 기능 jdbc func

 */
package sool.common.jdbc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lpad, regexp_replace}
import org.apache.spark.sql.functions.{col, concat_ws, rank, substring}
import org.apache.spark.sql.expressions.Window

class JdbcGet (spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  val jdbcPropCls = new JdbcProp()
  val (url, connProp) = jdbcPropCls.jdbcProp

  def allColumnTrim(df: DataFrame)={
    var newDf = df
    for(col <- df.columns){
      newDf = newDf.withColumnRenamed(col,col.trim())
    }
    newDf
  }
  // fix addr 테이블 로드
  def getFixAddr(tbNm: String) = {
    val itemTblPre = spark.read.jdbc(url, s"ADDRESS.${tbNm}", connProp)
    val itemTblFltr = itemTblPre.filter('USE_YN === "Y")
    val itemTbl = allColumnTrim(itemTblFltr.drop("DESC_TXT", "USE_YN", "REG_DT", "UPDT_DT"))
    itemTbl
  }

  // 아이템 테이블 로드
  def getItemTbl(tbNm: String) = {
    val itemTblPre = spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp)
    val itemTblFltr = itemTblPre.filter('USE_YN === "Y")
    val itemTbl = allColumnTrim(itemTblFltr.drop("DESC_TXT", "USE_YN", "REG_DT", "UPDT_DT"))
    itemTbl
  }

  // HJ 2차 HJ_AGRN_ITEM 테이블 로드
  def getHjAgrnItemTbl(tbNm: String) = {
    val tblRaw = spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp)
    val tblFltr = tblRaw.filter('USE_YN === "Y")
    val tbl = allColumnTrim(tblFltr.select("ITEM_NM", "MKT_CD"))
    tbl
  }

  // HJ 2차 HJ_AGRN_MKT 테이블 불러오기
  def getHjMktCdTbl(tbNm: String) = {
    val tblRaw = spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp)
    val tbl = allColumnTrim(tblRaw.select("MKT_CD", "MKT_NM"))
    tbl
  }

  // HK HK_AGRN_ITEM 테이블 로드
  def getHkAgrnItemTbl(tbNm: String) = {
    val hkAgrnItemTblRaw = spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp)
    val hkAgrnItemTblFltr = hkAgrnItemTblRaw.filter('USE_YN === "Y")
    val hkAgrnItemTbl = allColumnTrim(hkAgrnItemTblFltr.drop("ITEM_ID", "USE_YN", "REG_DT", "UPDT_DT"))
    hkAgrnItemTbl
  }

  // 분석 메뉴 개요 기본 테이블 로드
  def getMnIntrTbl(tbNm: String, ethDt: String) = {
    val mnIntrTblRaw = spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp)

    val mnIntrTblFil = if (tbNm == "HJ_MN_INTR") {
      mnIntrTblRaw.
        filter('USE_YN === "Y").
        filter('CRTN_YM <= ethDt)
    } else {
      mnIntrTblRaw.filter('USE_YN === "Y")    // DK
    }

    val mnIntrTbl = if (tbNm == "HJ_MN_INTR") {
      allColumnTrim(mnIntrTblFil.drop("CRTN_YM", "DESC_TXT", "USE_YN", "REG_DT", "UPDT_DT"))
    } else {
      allColumnTrim(mnIntrTblFil.drop("DESC_TEXT", "USE_YN", "REG_DT", "UPDT_DT"))   // DK
    }
    mnIntrTbl
  }

  // 필터 개요 기본 로드
  def getFltrIntrTbl(tbNm: String) = {
    val fltrIntrTblRaw = spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp)
    val fltrIntrTbl = allColumnTrim(fltrIntrTblRaw.drop("REG_DT", "UPDT_DT"))
    fltrIntrTbl
  }

  // 필터 규칙 목록
  def getFltrRLTbl(tbNm: String) = {
    val fltrRlTblRaw = spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp)
    val fltrRlTbl = allColumnTrim(fltrRlTblRaw.drop("REG_DT", "UPDT_DT"))
    fltrRlTbl
  }

  // 설명 포함 아이템 마스터 가져오기
  def getItemTblConDesc(tbNm: String) = {
    val itemTblPre = spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp)
    val itemTblFil = itemTblPre.filter('USE_YN === "Y")
    val itemTbl = allColumnTrim(itemTblFil.drop("USE_YN", "REG_DT", "UPDT_DT"))
    itemTbl
  }

  // 최신 분석 메뉴 개요 기본 가져오기 (comunication 용)
  def getCurrentMnIntr(tbNm: String) = {
    val mnIntrRaw = allColumnTrim(spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp).
      filter('USE_YN === "Y").
      drop("USE_YN", "REG_DT", "UPDT_DT", "DESC_TXT"))

    mnIntrRaw
  }

  // 아이템 엑셀 컬럼 가져오기
  def getItemColCD(SVC_ID: String, ITEM_TYPE_CD: String) = {
    val colNmRaw = spark.read.jdbc(url, "ITEM_COL_CD", connProp)
    val colNmTbl = allColumnTrim(colNmRaw.filter('SVC_ID === SVC_ID).
      filter('ITEM_TYPE_CD === ITEM_TYPE_CD).
      select('CD_ID, 'NW_COL_NM, 'PRE_COL_NM))
    colNmTbl
  }

  // 메뉴 엑셀 컬럼 가져오기
  def getMnColCD(SVC_ID: String) = {
    val colNmRaw = spark.read.jdbc(url, "MN_COL_CD", connProp)
    val colNmTbl = allColumnTrim(colNmRaw.filter('SVC_ID === SVC_ID).
      select('NW_COL_NM, 'PRE_COL_NM))
    colNmTbl
  }

  // LIQUOR 스키마 내 테이블을 추가 작업 없이 그대로 가져오기
  def getLiquorTbl(tbNm: String) = {
    allColumnTrim(spark.read.jdbc(url, s"LIQUOR.${tbNm}", connProp))
  }

  // ADDRESS 스키마 내 테이블을 추가 작업 없이 그대로 가져오기
  def getAddressTbl(tbNm: String) = {
    allColumnTrim(spark.read.jdbc(url, s"ADDRESS.${tbNm}", connProp))
  }

  // ADDRESS.BLDNG_INFO_VIEW
  // todo: 1000만 건 이상의 데이터를 불러오는 거라 느릴 수 있다. 일단 파티션 나눠서 불러오는 걸로 해놓은 상태인데 더 좋은 방안이 있으면 개선할 것.
  def getBldngInfoView() = {
//    val (user, password, url) = jdbcPropCls.soolAdrsDbInfo()
    val (conn80, stat80, jdbcUrl80, ckProperties80) = jdbcPropCls.clickHouseStat80()
    val MOIS_BLDG_ADDR = spark.read.option("driver", "ru.yandex.clickhouse.ClickHouseDriver").jdbc(jdbcUrl80, "MOIS_BLDG_ADDR", ckProperties80)

    val bldngInfoViewLoad = MOIS_BLDG_ADDR.
      withColumnRenamed("SD_NM", "SIDO_NM").
      withColumnRenamed("SGG_NM", "SIGUNGU_NM").
      withColumnRenamed("BJD_NM", "LEGAL_EPMYNDNG_NM").
      withColumnRenamed("JIBN_SB_NO", "JIBEON_BUBEON_HO").
      withColumnRenamed("JIBN_MA_NO","JIBEON_BONBEON_BEONJI").
      withColumnRenamed("RD_NM", "ROAD_NM").
      withColumnRenamed("BD_MA_NO", "BLDNG_BONBEON").
      withColumnRenamed("BD_SB_NO", "BLDNG_BUBEON").
      withColumnRenamed("BD_MNG_NO", "BLDNG_MGMT_NMB").
      withColumnRenamed("ZIP_CD", "POST_CD").
      withColumn("JIBEON", concat_ws("-", col("JIBEON_BONBEON_BEONJI"), col("JIBEON_BUBEON_HO"))).
      withColumn("BLDNG_BON_BUBEON", concat_ws("-", col("BLDNG_BONBEON"), col("BLDNG_BUBEON"))).
      withColumn("PNU", substring(col("BLDNG_MGMT_NMB"), 1, 19))
    val w = Window.partitionBy("BLDNG_MGMT_NMB").orderBy($"UPDT_TIME".desc, $"IDX".desc)
    val bldngInfoView = bldngInfoViewLoad.
            withColumn("POST_CD", lpad('POST_CD, 5, "0")).
            withColumn("JIBEON", regexp_replace('JIBEON, "-0", "")).  // ex: 1-0 (x) ---> 1 (o)
            withColumn("BLDNG_BON_BUBEON", regexp_replace('BLDNG_BON_BUBEON, "-0", "")).
            withColumn("RANK", rank.over(w)).
            filter('RANK === 1).
            drop("RANK")
    bldngInfoView
  }
}