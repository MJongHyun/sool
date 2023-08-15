/**
 * DK(DK) 마트 생성

 */
package sool.service.get_marts.get_mart_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, when}
import sool.service.run.RunService.logger
import sool.common.path.FilePath
import sool.common.jdbc.JdbcGet
import sool.service.get_marts.get_mart_dk._

class RunGetMartDk(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val jdbcGetCls = new JdbcGet(spark)
  val saveMartDkCls = new SaveMartDk(spark)

  def getDkDfs(ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)

    // DB, DK beer, whisky master
    val dkBrItem = jdbcGetCls.getItemTbl("DK_BR_ITEM")
    val dkWskItem = jdbcGetCls.getItemTbl("DK_WSK_ITEM")

    // WHOLE_SALE
    val dkWholeSalePre = spark.read.parquet(filePathCls.dkWholeSalePath)
    val dkWholeSale = dkWholeSalePre.
      withColumnRenamed("COM_RGNO", "BYR_RGNO").
      withColumn("SUP", lit("YES"))

    // com_main_dk
    val comMainDkPre = spark.read.parquet(filePathCls.comMasterDkPath)
    val comMainDk = comMainDkPre.
      filter('ADDR1.isNotNull).
      withColumn("ADDR2", when('ADDR1 === "세종특별자치시", "전체").otherwise('ADDR2)).
      select('COM_RGNO as "BYR_RGNO", 'COM_NM as "BYR_NM", 'ADDR1, 'ADDR2, 'ADDR3)

    // base_tagged
    val baseTagged = spark.read.parquet(filePathCls.baseTaggedPath)
    (dkBrItem, dkWskItem, dkWholeSale, comMainDk, baseTagged)
  }

  def getMartDk(itemTbl:DataFrame, dkWholeSale:DataFrame, comMainDk:DataFrame, baseTagged:DataFrame) = {
    val martDkPre1 = baseTagged.
      join(dkWholeSale, Seq("BYR_RGNO"), "left_outer").
      filter('SUP.isNull).drop("SUP").
      select('BYR_RGNO, 'ITEM, 'ITEM_SZ, 'ITEM_QT, 'SUP_AMT)
    val martDkPre2 = martDkPre1.join(comMainDk, Seq("BYR_RGNO"))

    val itemTblCols = itemTbl.columns
    val itemTblColNm = if (itemTblCols.contains("BR_NM")) "BR_NM" else if (itemTblCols.contains("WSK_NM")) "WSK_NM" else ""

    val martDk = martDkPre2.
      join(itemTbl, martDkPre2("ITEM") === itemTbl(itemTblColNm)).
      withColumn("QT", 'ITEM_QT * 'VSL_SIZE)
    martDk
  }

  def runGetMartDk(ethDt:String, flag: String) = {
    logger.info("[appName=sool] [function=runGetMartDk] [runStatus=start] [message=start]")

    // 데이터 준비
    val (dkBrItem, dkWskItem, dkWholeSale, comMainDk, baseTagged) = getDkDfs(ethDt, flag)

    // 맥주, 위스키 마트 생성
    val martDkb = getMartDk(dkBrItem, dkWholeSale, comMainDk, baseTagged)
    val martDkw = getMartDk(dkWskItem, dkWholeSale, comMainDk, baseTagged)
    saveMartDkCls.saveMartDk(martDkb, martDkw, ethDt, flag)  // 저장

    logger.info("[appName=sool] [function=runGetMartDk] [runStatus=end] [message=end]")
  }
}
