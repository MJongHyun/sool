/**
 * AG 결과 생성
j
 */
package sool.service.aggregate.agg_anglnt

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, row_number, when}
import sool.service.run.RunService.logger
import sool.common.path.FilePath
import sool.common.jdbc.JdbcGet
import org.apache.spark.sql.expressions.Window

class RunAggAnglNt(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val jdbcGetCls = new JdbcGet(spark)

  // AG 아이템 마스터 추출
  def getAnglNtMaster(ethDt:String) = {
    // DB, DK beer, whisky master
    val dkBrItem = jdbcGetCls.getItemTbl("DK_BR_ITEM")
    val dkWskItem = jdbcGetCls.getItemTbl("DK_WSK_ITEM")

    val anglNtBrItem = dkBrItem.
      filter('DIST_NM === "Diageo").
      select('BR_ID as "PROD_CD",
        'BR_NM as "PROD_NM",
        'VSL_SIZE as "VOL").
      withColumn("TYPE", lit("N"))

    val anglNtWskItem = dkWskItem.
      filter('DIST_NM === "Diageo Korea").
      filter('WSK_ID.isNotNull).
      withColumn("TYPE", when($"PROD_NM" === "Int'l", "N").otherwise("L")).
      select('WSK_ID as "PROD_CD",
        'WSK_NM as "PROD_NM",
        'VSL_SIZE as "VOL",
        'TYPE)

    val anglNtMaster = anglNtBrItem.union(anglNtWskItem).
      withColumn("USE_CD", lit("2"))

    anglNtMaster
  }

  // baseTagged 추출
  def getBaseTagged(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val baseTagged = spark.read.parquet(filePathCls.baseTaggedPath).drop("TYPE")  // base_tagged
    baseTagged
  }

  // AG 결과 생성
  def getAggAnglNt(anglNtMaster:DataFrame, baseTagged:DataFrame) = {
    val anglNtData = baseTagged.join(anglNtMaster, $"ITEM" === $"PROD_NM").
      withColumn("CNT", $"ITEM_QT".cast("Int"))

    val w =  Window.partitionBy("WR_DT", "BYR_RGNO").orderBy("DATE")
    val anglNtSeq = anglNtData.withColumn("SEQ", row_number().over(w))
    val anglNtRslt = anglNtSeq.
      orderBy("DATE", "BYR_RGNO", "SEQ").
      select('WR_DT as "YM",
        'DATE as "WR_DT",
        'SEQ,
        'BYR_RGNO as "BIZ_NO",
        'PROD_NM,
        'PROD_CD,
        'VOL,
        'USE_CD,
        'TYPE,
        'CNT,
        'ITEM_UP as "COST",
        'SUP_AMT as "SUP",
        'TAX_AMT as "VAT")

    anglNtRslt
  }

  // 메인
  def runAggAnglNt(ethDt: String, flag: String) = {
    logger.info("[appName=sool] [function=runAggAnglNt] [runStatus=start] [message=start]")

    // 데이터 준비
    val anglNtMaster = getAnglNtMaster(ethDt)
    val baseTagged = getBaseTagged(ethDt, flag)

    // AG 결과 생성
    val anglNtRslt = getAggAnglNt(anglNtMaster, baseTagged)
    new SaveAnglNtRslt(spark).saveAnglNtRslt(anglNtRslt) // 저장, 58번 서버 to DK TEST DB 안되는 거 같음(21.07.21)

    logger.info("[appName=sool] [function=runAggAnglNt] [runStatus=end] [message=end]")
  }
}
