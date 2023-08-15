/**
 * HJ 2차 집계를 위한 필요 데이터들 로드 및 생성

 */
package sool.service.aggregate.agg_hj2

import org.apache.spark.sql.functions.{lit, col, when}
import sool.common.path.FilePath
import sool.common.jdbc.JdbcGet

class GetAggDfsHj2(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  val jdbcGet = new JdbcGet(spark)

  // HJ 2차 아이템 마트 생성
  def getMartHj2(ethDt:String, flag:String) = {
    // HJ 2차 아이템 리스트 생성
    // [2022.10.03] bzCd 관련 데이터 가져오기
    // bzCd 아이템 불러오기
    val hjbBzCdItem = jdbcGet.getItemTbl("HJ_BR_ITEM").filter(col("BR_ID").contains("BZ"))
    val hjsBzCdItem = jdbcGet.getItemTbl("HJ_SJ_ITEM").filter(col("SJ_ID").contains("BZ"))
    // bzCd 아이템들 중 각 아이템별 분석코드에 해당되는 값 추출
    val beerBzItemMktpre = hjbBzCdItem.withColumnRenamed("BR_NM", "ITEM_NM").
      withColumn("MKT_CD_PRE",
        when(col("VSL_TYPE_CD") === "HJBCONT3" && col("PROD_CD") === "HJBLOCIN1", lit("B01")).
        when(col("VSL_TYPE_CD") === "HJBCONT1" && col("VSL_SIZE") === "330" && col("PROD_CD") === "HJBLOCIN1", lit("B02")).
        when(col("VSL_TYPE_CD") === "HJBCONT1" && col("VSL_SIZE") === "500" && col("PROD_CD") === "HJBLOCIN1", lit("B03")).
        when(col("VSL_TYPE_CD") === "HJBCONT3" && col("PROD_CD") === "HJBLOCIN2", lit("B05")).
        when(col("VSL_TYPE_CD") === "HJBCONT1" && col("VSL_SIZE") === "330" && col("PROD_CD") === "HJBLOCIN2", lit("B06")).
        otherwise(lit("B00")))

    val beerBzItemMkt = beerBzItemMktpre.withColumn("MKT_CD",
      when(col("MKT_CD_PRE") === "B00" && col("PROD_CD") === "HJBLOCIN1", lit("B04")).
      when(col("MKT_CD_PRE") === "B00" && col("PROD_CD") === "HJBLOCIN2", lit("B07")).
      otherwise(col("MKT_CD_PRE"))).
      select("ITEM_NM", "MKT_CD")

    val sojuBzItemMkt = hjsBzCdItem.
      filter(col("SJ_TYPE_CD") =!= "HJSTYPE7" && col("SJ_TYPE_CD") =!= "HJSTYPE6").
      withColumnRenamed("SJ_NM", "ITEM_NM").
      withColumn("MKT_CD",
        when(col("SJ_TYPE_CD") =!= "HJSTYPE4" && col("VSL_SIZE") === "360", lit("S01")).
        when(col("SJ_TYPE_CD") =!= "HJSTYPE4" && col("VSL_SIZE") =!= "360", lit("S02")).
        otherwise(lit("S03"))).
      select("ITEM_NM", "MKT_CD")
    // bzCd로 추출한 데이터와 기존 HJ2차 아이템과 합쳐서 최종 마스터 추출
    val bzCdItemMktPre = beerBzItemMkt.union(sojuBzItemMkt)
    val hj2MktCd = jdbcGet.getHjMktCdTbl("HJ_MKT_CD")
    val bzCdItemMkt = bzCdItemMktPre.
      join(hj2MktCd, Seq("MKT_CD")).
      withColumnRenamed("ITEM_NM", "ITEM")
    val hj2AgrnItem = jdbcGet.getHjAgrnItemTbl("HJ_AGRN_ITEM")
    val hj2ItemMkt = hj2AgrnItem.
      join(hj2MktCd, Seq("MKT_CD")).
      withColumnRenamed("ITEM_NM", "ITEM").
      union(bzCdItemMkt)

    val filePathCls = new FilePath(ethDt, flag)

    // mart_hite
    val martHjb = spark.read.parquet(filePathCls.hjrMartBPath)
    val martHjs = spark.read.parquet(filePathCls.hjrMartSPath)

    // HJ 2차 아이템 마트
    val martHjb2 = martHjb.join(hj2ItemMkt, Seq("ITEM"))
    val martHjs2 = martHjs.join(hj2ItemMkt, Seq("ITEM"))
    (martHjb2, martHjs2)
  }

  // HJ 2차 결과 생성을 위해 필요한 데이터들
  def getDfsHj2(ethDt:String, flag: String) = {
    // HJ 2차 아이템 마트
    val filePathCls = new FilePath(ethDt, flag)

    // com_main
    val comMain = spark.read.parquet(filePathCls.comMainPath)

    // 주소 데이터 필터 및 필요한 컬럼만 셀렉트
    val comMainFil = comMain.filter('ADDR_TYPE==="NEW" || 'ADDR_TYPE==="FIX").filter('LEN > 0)
    val comMainFilSel = comMainFil.select('COM_RGNO, 'ADDR1).withColumn("EXIST", lit("Y"))

    // territory_order
    val territoryOrder = spark.read.parquet(filePathCls.hj2TerritoryOrderPath)

    (comMain, comMainFilSel, territoryOrder)
  }

  def getNewCom202105Hj2() = {
    val newCom202105Hj2Path = "s3a://sool/HJ2/Result/202105/new/Data/com.parquet"
    val newCom202105Hj2 = spark.read.parquet(newCom202105Hj2Path)
    newCom202105Hj2
  }
}
