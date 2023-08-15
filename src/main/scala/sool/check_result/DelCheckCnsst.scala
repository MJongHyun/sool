/**
 * (삭제 예정 파일, 확인용으로 잠깐 생성함)
 * 기존 코드로 생성한 마트와 새로운 코드로 생성한 마트 비교
 * author:
 */
package sool.check_result

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, when}
import org.apache.spark.sql.types.DecimalType
import sool.common.function.FileFunc
import sool.common.jdbc.JdbcGet

class DelCheckCnsst(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  def s3() = {
    import org.apache.spark.sql.SparkSession

    val spark: SparkSession = SparkSession.
      builder.
      appName("sool").
      getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "") // sool 서버에 있던 key
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "") // sool 서버에 있던 key
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") //  로컬에 필요 했었음
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") //  로컬에 필요 했었음
  }

  def checkExtrct() = {
    val oldDf = spark.read.parquet("s3a://sool/Base/202109/Data/backup/base.parquet")
    val newDf = spark.read.parquet("s3a://sool/Base/202109/Data/base.parquet")

    oldDf.count - newDf.count
    oldDf.except(newDf).count
    newDf.except(oldDf).count
  }

  // DK
  def checkMartDk() = {
    val oldDf = spark.read.parquet("/user/sool/DK/Result/202105/old/mart_dk.parquet")
    val newDf1 = spark.read.parquet("/user/sool/DK/Result/202105/mart_dk_beer.parquet")
    val newDf2 = spark.read.parquet("/user/sool/DK/Result/202105/mart_dk_whisky.parquet")

    val oldDfCols = oldDf.columns
    val newDf1Cols = newDf1.columns
    val newDf2Cols = newDf2.columns

    val commonCols = oldDfCols.intersect(newDf1Cols).intersect(newDf2Cols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDf1S = newDf1.select(commonCols:_*)
    val newDf2S = newDf2.select(commonCols:_*)
    val newDfS = newDf1S.union(newDf2S)

    oldDfS.count - newDfS.count
    oldDfS.except(newDfS).count
    newDfS.except(oldDfS).count
  }

  // DK
  def checkMartHj() = {
    val oldDf = spark.read.parquet("/user//sool/data/DK/Result/202103/mart_dk.parquet")
    val newDf1 = spark.read.parquet("/user//sool/data/DK/Result/202103/mart_dk_beer.parquet")
    val newDf2 = spark.read.parquet("/user//sool/data/DK/Result/202103/mart_dk_whisky.parquet")

    val oldDfCols = oldDf.columns
    val newDf1Cols = newDf1.columns
    val newDf2Cols = newDf2.columns

    val commonCols = oldDfCols.intersect(newDf1Cols).intersect(newDf2Cols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDf1S = newDf1.select(commonCols:_*)
    val newDf2S = newDf2.select(commonCols:_*)
    val newDfS = newDf1S.union(newDf2S)

    oldDfS.count - newDfS.count
    oldDfS.except(newDfS).count
    newDfS.except(oldDfS).count
  }

  // DK
  def checkDenoNumeMartHj() = {
    val oldDf = spark.read.parquet("/user/sool/DK/Result/202105/old/DK_DENOMINATOR_MART.parquet")
    val newDf1 = spark.read.parquet("/user/sool/DK/Result/202105/DK_DENOMINATOR_MART_B.parquet")
    val newDf2 = spark.read.parquet("/user/sool/DK/Result/202105/DK_DENOMINATOR_MART_W.parquet")

    val oldDfCols = oldDf.columns
    val newDf1Cols = newDf1.columns
    val newDf2Cols = newDf2.columns

    val commonCols = oldDfCols.intersect(newDf1Cols).intersect(newDf2Cols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDf1S = newDf1.select(commonCols:_*)
    val newDf2S = newDf2.select(commonCols:_*)
    val newDfS = newDf1S.union(newDf2S)

    oldDfS.count
    newDfS.count
    oldDfS.count - newDfS.count
    oldDfS.except(newDfS).count
    newDfS.except(oldDfS).count
  }

  // HK
  def checkCnsst2() = {
    val oldDf = spark.read.parquet("/home//sool/data/HNK/Result/202103/mart_hk_202103.parquet")
    val newDf = spark.read.parquet("/home//sool/data/HNK/Result/202103/mart_hnk.parquet/part-00000-f733cf76-7f04-46ee-a63a-8df3dca8e75c-c000.snappy.parquet")

    val oldDfCols = oldDf.columns
    val newDfCols = newDf.columns

    val commonCols = oldDfCols.intersect(newDfCols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDfS = newDf.select(commonCols:_*)

    oldDfS.count - newDfS.count
    oldDfS.except(newDfS).count
    newDfS.except(oldDfS).count

    newDfS.drop("HNK_CATEGORY", "BRAND").except(oldDfS.drop("HNK_CATEGORY", "BRAND")).count
    oldDfS.drop("HNK_CATEGORY", "BRAND").except(newDfS.drop("HNK_CATEGORY", "BRAND")).count
  }

  def checkDkExcel() = {
    val oldDf = spark.read.parquet("/user//sool/data/DK/Result/202103/viewResult_EXCEL_202103_old.parquet").
      filter('YM === "F21P09").
      withColumn("DEALER_MARKET", when('DEALER_MARKET === "-", 'DEALER_MARKET).otherwise('DEALER_MARKET.cast(DecimalType(18,2))))
    val newDf = spark.read.parquet("/user//sool/data/DK/Result/202103/viewResult_EXCEL_202103.parquet").
      withColumn("DEALER_MARKET", when('DEALER_MARKET === "-", 'DEALER_MARKET).otherwise('DEALER_MARKET.cast(DecimalType(18,2))))

    val oldDfCols = oldDf.columns
    val newDfCols = newDf.columns

    val commonCols = oldDfCols.intersect(newDfCols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDfS = newDf.select(commonCols:_*)
    val oldExceptNew = oldDfS.except(newDfS)

    oldDfS.count
    newDfS.count
    oldDfS.count - newDfS.count
    oldDfS.except(newDfS).count
    newDfS.except(oldDfS).count
  }

  // DK RESULT 비교
  def checkDkMenuAnlysisRes() = {
    val oldDf = spark.read.parquet("/user/sool/DK/Result/202105/old/menuAnalysisResultDF.parquet").
      filter('MENU_ID =!= "B40020100").
      filter('MENU_ID =!= "B40010200").
      filter('MENU_ID =!= "B40010100").
      filter('MENU_ID =!= "B40020200").
      filter('MENU_ID =!= "B40020300")
    val newDf = spark.read.parquet("/user/sool/DK/Result/202105/menuAnalysisResultDF.parquet").
      filter('MENU_ID =!= "B40020100").
      filter('MENU_ID =!= "B40010200").
      filter('MENU_ID =!= "B40010100").
      filter('MENU_ID =!= "B40020200").
      filter('MENU_ID =!= "B40020300")

    val oldDfCols = oldDf.columns
    val newDfCols = newDf.columns

    val commonCols = oldDfCols.intersect(newDfCols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDfS = newDf.select(commonCols:_*)
    val oldExceptNew = oldDfS.except(newDfS)
    val newExceptOld = newDfS.except(oldDfS)

    oldDfS.count
    newDfS.count
    oldDfS.count - newDfS.count
    oldExceptNew.count
    newExceptOld.count
  }

  // DK Web RESULT 비교
  def checkDkWebRes() = {

    val oldDf = spark.read.parquet("/user/sool/DK/Result/202105/old/viewResult_WEB_202105.parquet").
      filter('FISCAL_YEAR === "F21").
      filter('MENU_ID =!= "B40020100").
      filter('MENU_ID =!= "B40010200").
      filter('MENU_ID =!= "B40010100").
      filter('MENU_ID =!= "B40020200").
      filter('MENU_ID =!= "B40020300")
//      .
//      join(exceptDf, exceptDfCols, "leftanti").drop('DATA_DEALER)

    val newDf = spark.read.parquet("/user/sool/DK/Result/202105/viewResult_WEB_202105.parquet").
      filter('MENU_ID =!= "B40020100").
      filter('MENU_ID =!= "B40010200").
      filter('MENU_ID =!= "B40010100").
      filter('MENU_ID =!= "B40020200").
      filter('MENU_ID =!= "B40020300")
//      .
//      join(exceptDf, exceptDfCols, "leftanti").drop('DATA_DEALER)

    oldDf.columns.size
    newDf.columns.size

    val oldExceptNew = oldDf.except(newDf)
    val newExceptOld = newDf.except(oldDf)

//    newDf.filter('MENU_ID === "B10000000" && 'ADDRESS === "상동 송내대로73번길").show(false)
//    newDf.filter('MENU_ID === "B20020000" && 'ADDRESS === "연남동 성미산로29안길").show(false)

    oldDf.count
    newDf.count
    oldDf.count - newDf.count
    oldExceptNew.count
    newExceptOld.count
  }

  // DK RESULT 비교
  def checkDkExcelRes() = {
    val oldDf = spark.read.parquet("/user/sool/DK/Result/202105/old/viewResult_EXCEL_202105.parquet").
      filter('YM.contains("F21P11")).
      filter('MENU_ID =!= "B40020100").
      filter('MENU_ID =!= "B40010200").
      filter('MENU_ID =!= "B40010100").
      filter('MENU_ID =!= "B40020200").
      filter('MENU_ID =!= "B40020300")
//      .
//      withColumn("DEALER_MARKET", when('DEALER_MARKET === "-", 'DEALER_MARKET).otherwise('DEALER_MARKET.cast(DecimalType(18,2))))

    val newDf = spark.read.parquet("/user/sool/DK/Result/202105/viewResult_EXCEL_202105.parquet").
      filter('MENU_ID =!= "B40020100").
      filter('MENU_ID =!= "B40010200").
      filter('MENU_ID =!= "B40010100").
      filter('MENU_ID =!= "B40020200").
      filter('MENU_ID =!= "B40020300")
//      .
//      withColumn("DEALER_MARKET", when('DEALER_MARKET === "-", 'DEALER_MARKET).otherwise('DEALER_MARKET.cast(DecimalType(18,2))))
//
//    val chngCols = Seq(
//      col("YM"), col("MENU_ID"), col("ADDR_LVL"), col("ADDR1"), col("ADDR2"), col("ADDRESS"),
//      when('QT_MARKET === "-0.00", "0.00").otherwise('QT_MARKET).as("QT_MARKET"),
//      when('QT_SOM === "-0.00", "0.00").otherwise('QT_SOM).as("QT_SOM"),
//      when('AMT_MARKET === "-0.00", "0.00").otherwise('AMT_MARKET).as("AMT_MARKET"),
//      when('AMT_SOM === "-0.00", "0.00").otherwise('AMT_SOM).as("AMT_SOM"),
//      when('BOTTLE_MARKET === "-0.00", "0.00").otherwise('BOTTLE_MARKET).as("BOTTLE_MARKET"),
//      when('BOTTLE_SOM === "-0.00", "0.00").otherwise('BOTTLE_SOM).as("BOTTLE_SOM"),
//      when('DEALER_MARKET === "-0.00", "0.00").otherwise('DEALER_MARKET).as("DEALER_MARKET"),
//      when('DEALER_SOM === "-0.00", "0.00").otherwise('DEALER_SOM).as("DEALER_SOM")
//    )

    oldDf.columns.size
    newDf.columns.size

    val oldExceptNew = oldDf.except(newDf)
    val newExceptOld = newDf.except(oldDf)

    oldDf.count
    newDf.count
    oldDf.count - newDf.count
    oldExceptNew.count
    newExceptOld.count
  }

  // DK RESULT 비교
  def checkDkDenoNume() = {
    val oldDf = spark.read.parquet("/user/sool/DK/Result/202105/old/denominatorDF.parquet")
    val newDf = spark.read.parquet("/user/sool/DK/Result/202105/denominatorDF.parquet")

    val oldDfCols = oldDf.columns
    val newDfCols = newDf.columns

    val commonCols = oldDfCols.intersect(newDfCols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDfS = newDf.select(commonCols:_*)
    val oldExceptNew = oldDfS.except(newDfS)
    val newExceptOld = newDfS.except(oldDfS)

    oldDfS.count
    newDfS.count
    oldDfS.count - newDfS.count
    oldExceptNew.count
    newExceptOld.count
  }

  // HK RESULT 비교
  def checkHkRes() = {
    val oldDf = spark.read.parquet("/Users//sool/data/HK/Result/202103/resultHK_old.parquet")
    val newDf = spark.read.parquet("/Users//sool/data/HK/Result/202103/resultHK.parquet")

    val oldDfCols = oldDf.columns
    val newDfCols = newDf.columns

    val commonCols = oldDfCols.intersect(newDfCols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDfS = newDf.select(commonCols:_*)
    val oldExceptNew = oldDfS.except(newDfS)
    val newExceptOld = newDfS.except(oldDfS)

    oldDfS.count
    newDfS.count
    oldDfS.count - newDfS.count
    oldExceptNew.count
    newExceptOld.count
  }

  def checkHjFltrQry(hjMnIntr: DataFrame, hjFltrQry: DataFrame, oldDfS: DataFrame, newDfS: DataFrame) = {
    val hjMnIntrSlct = hjMnIntr.select('MN_NM, 'FLTR_ID1, 'FLTR_ID2)
    val hjFltrQryDeno = hjFltrQry.select('FLTR_ID.as("FLTR_ID1"), 'QRY_C.as("QRY_ID1"))
    val hjFltrQryNume = hjFltrQry.select('FLTR_ID.as("FLTR_ID2"), 'QRY_C.as("QRY_ID2"))

    val hjMnQryPre = hjMnIntrSlct.join(hjFltrQryDeno, Seq("FLTR_ID1")).join(hjFltrQryNume, Seq("FLTR_ID2"))
    val hjMnQry = hjMnQryPre.select("MN_NM", "FLTR_ID1", "QRY_ID1", "FLTR_ID2", "QRY_ID2")

    val hjMnQryDeno = hjMnQry.select('MN_NM, 'FLTR_ID1, 'QRY_ID1).orderBy('MN_NM)
    val hjMnQryNume = hjMnQry.select('MN_NM, 'FLTR_ID2, 'QRY_ID2).orderBy('MN_NM)

    // old - new
    oldDfS.except(newDfS).select('NUMER_CD, 'DENOM_CD).distinct.as[(String, String)].collect.map(i => {
      val (nume, deno) = (i._1, i._2)
      hjMnQry.filter('FLTR_ID1 === deno).filter('FLTR_ID2 === nume).show(false)
      }
    )
  }

  def checkDkAggRes = {
    val denominatorDF = spark.read.parquet("/Users//sool/data/DK/Result/202103/denominatorDF.parquet")
    val martDkDenoAddrG = spark.read.parquet("/Users//sool/data/DK/Result/202103/martDkDenoAddrG.parquet")
    val martDkNumeAddrGFinal = spark.read.parquet("/Users//sool/data/DK/Result/202103/martDkNumeAddrGFinal.parquet")
    val denoGap = denominatorDF.except(martDkDenoAddrG)
  }

  def checkHj2 = {
    val compInfo = spark.read.option("header", "true").
      option("delimiter", "\t").
      csv("s3a://sool/HJ2/Result/202104/File/COMP_INFO.tsv")

    val compInfoOld = spark.read.format("com.crealytics.spark.excel").
      option("header", "true").option("delimiter", "\t").
      load("s3a://sool/HJ2/Result/202104/new2/COMP_INFO.xlsx").
      withColumn("QT_TERRITORY", 'QT_TERRITORY.cast(DecimalType(18,2))).
      withColumn("AMT_TERRITORY", 'AMT_TERRITORY.cast(DecimalType(18,2)))

    val anlyResult = spark.read.option("header", "true").option("delimiter", "\t").csv("s3a://sool/HJ2/Result/202104/File/ANALIYSIS_RESULT.tsv")

    val anlyResultOld = spark.read.option("header", "true").option("delimiter", "\t").
      csv("s3a://sool/HJ2/Result/202104/new2/File/ANALIYSIS_RESULT.tsv")
  }

  // HK RESULT 비교
  def checkHj2Res() = {
    val oldDf = spark.read.parquet("s3a://sool/HJ2/Result/202104/new2/Data/mart_hite2.parquet")
    val newDf = spark.read.parquet("s3a://sool/HJ2/Result/202104/Data/mart_hite2.parquet")

    val oldDfCols = oldDf.columns
    val newDfCols = newDf.columns

    val commonCols = oldDfCols.intersect(newDfCols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDfS = newDf.select(commonCols:_*)
    val oldExceptNew = oldDfS.except(newDfS)
    val newExceptOld = newDfS.except(oldDfS)

    oldDfS.count
    newDfS.count
    oldDfS.count - newDfS.count
    oldExceptNew.count
    newExceptOld.count
  }

  // HK RESULT 비교
  def checkHjTsvRes() = {
    val fileFuncCls = new FileFunc(spark)

    val oldDf = fileFuncCls.rTsv("s3a://sool/HJ/Result/202106/aggResult/hite202106/ANVR06.tsv")
    val newDf = fileFuncCls.rTsv("s3a://sool/HJ/Result/202106/aggResult/hite202106/Before2_TEST_DEL/ANVR06.tsv")

    val oldDfCols = oldDf.columns
    val newDfCols = newDf.columns

    val commonCols = oldDfCols.intersect(newDfCols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDfS = newDf.select(commonCols:_*)
    val oldExceptNew = oldDfS.except(newDfS)
    val newExceptOld = newDfS.except(oldDfS)

    oldExceptNew.count
    newExceptOld.count
    oldDfS.count
    newDfS.count
    oldDfS.count - newDfS.count
  }

  // HJB, DB 마스터랑 엑셀 마스터랑 비교하기
  def checkHjBrItem = {
    val jdbcGetCls = new JdbcGet(spark)
    val fileFuncCls = new FileFunc(spark)

    // DB 마스터
    val hjBrItem = jdbcGetCls.getItemTbl("HJ_BR_ITEM")
    val hjItemColCd = jdbcGetCls.getItemColCD("HJ", "B")
    val hjItemColCdMap = hjItemColCd.drop("CD_ID").collect.map(_.toSeq).map(x => (x(0), x(1))).toMap
    val hjBrItemCols = hjBrItem.columns.map(x => hjItemColCdMap(x).toString).toSeq
    val hjBrItemRename = hjBrItem.toDF(hjBrItemCols:_*)

    // 기존 엑셀 마스터
    val hjbItemMaster = fileFuncCls.rXlsx("s3a://sool-test//HJB_ITEM_MASTER.xlsx", "HJBEER").
      drop("NO", "HJB_REMARK")
    val hjBrItemColsOrder = hjbItemMaster.columns.toSeq.map(col)

    // DB 마스터 컬럼 순서를 기존 엑셀 마스터 컬럼 순서랑 맞추기
    val hjBrItemFinal = hjBrItemRename.select(hjBrItemColsOrder:_*)

    hjBrItemFinal.except(hjbItemMaster).count()
    hjbItemMaster.except(hjBrItemFinal).count()
  }

  def dkExceptMn = {
    val old = spark.read.parquet("s3a://sool/DK/Result/202107/menuAnalysisResultDF.parquet")
    val newDf = spark.read.parquet("s3a://sool-test//del_test2/menuAnalysisResultDF.parquet")
    val oldToNew = old.except(newDf)

    old.select('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3, 'DEAL_MARKET_DK).except(newDf.select('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3, 'DEAL_MARKET_DK)).count
    old.select('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3, 'DEAL_SOM).except(newDf.select('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3, 'DEAL_SOM)).count
    old.select('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3, 'DEAL_GAP).except(newDf.select('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3, 'DEAL_GAP)).count

    val oldToNewSlct = oldToNew.select('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3, 'DEAL_MARKET_DK, 'DEAL_SOM, 'DEAL_GAP)
    val newSlct = newDf.select('MENU_ID, 'ADDR_LVL, 'ADDR1, 'ADDR2, 'ADDR3, 'DEAL_MARKET_DK.as("DEAL_MARKET_DK_NEW"), 'DEAL_SOM.as("DEAL_SOM_NEW"), 'DEAL_GAP.as("DEAL_GAP_NEW"))
    val oldToNewJoin = oldToNewSlct.join(newSlct, Seq("MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDR3"))

    val oldToNewGap = oldToNewJoin.
      withColumn("DEAL_MARKET_DK_GAP", 'DEAL_MARKET_DK - 'DEAL_MARKET_DK_NEW).
      withColumn("DEAL_SOM_GAP", 'DEAL_SOM - 'DEAL_SOM_NEW).
      withColumn("DEAL_GAP_GAP", 'DEAL_GAP - 'DEAL_GAP_NEW).
      withColumn("DEAL_MARKET_DK_GAP_BOOLEAN", 'DEAL_MARKET_DK_GAP >= 1)
  }

  // HJ 3 ~ 5월 재집계 마트 비교
  def checkHj03to05() = {
    val ym = "202103"
    val oldDf = spark.read.parquet(s"s3a://sool/HJ/Result/${ym}/mart_hite.parquet")
    val newDf = spark.read.parquet(s"s3a://sool/HJ/Result/${ym}/20211001_paulaner/mart_hite.parquet")
    oldDf.count - newDf.count

    val oldToNew = oldDf.except(newDf)
    val newToOld = newDf.except(oldDf)
    oldToNew.count
    newToOld.count

    oldToNew.select('ITEM).groupBy('ITEM).agg(count('ITEM).as("CNT")).show
    newToOld.select('ITEM).groupBy('ITEM).agg(count('ITEM).as("CNT")).show
  }

  // HJ 6 ~ 8월 재집계 마트 비교
  def checkHj06to08() = {
    val ym = "202108"

    val oldDf1 = spark.read.parquet(s"s3a://sool/HJ/Result/${ym}/mart_hite_soju.parquet")
    val oldDf2 = spark.read.parquet(s"s3a://sool/HJ/Result/${ym}/mart_hite_beer.parquet")
    val newDf = spark.read.parquet(s"s3a://sool/HJ/Result/${ym}/20211001_paulaner/mart_hite.parquet")

    val oldDf1Cols = oldDf1.columns
    val oldDf2Cols = oldDf2.columns
    val newDfCols = newDf.columns

    val commonCols = newDfCols.intersect(oldDf1Cols).intersect(oldDf2Cols).map(col)

    val oldDf1S = oldDf1.select(commonCols:_*)
    val oldDf2S = oldDf2.select(commonCols:_*)
    val oldDfS = oldDf1S.union(oldDf2S)
    val newDfS = newDf.select(commonCols:_*)
    oldDfS.count - newDfS.count

    val oldToNew = oldDfS.except(newDfS)
    val newToOld = newDfS.except(oldDfS)
    oldToNew.select('ITEM).groupBy('ITEM).agg(count('ITEM).as("CNT")).show
    newToOld.select('ITEM).groupBy('ITEM).agg(count('ITEM).as("CNT")).show
  }

  // HJ 재집계 결과 비교
  def checkHjDfs2() = {
    import org.apache.spark.sql.DataFrame

    def rXlsx(path: String, sheetName: String = "Sheet1"): DataFrame = {
      spark.read.format("com.crealytics.spark.excel").
        option("sheetName", s"$sheetName").
        option("header", "true").
        load(s"${path}")
    }

    val verify = rXlsx("s3a://sool-test/202108/CHANGE_MENU.xlsx", "기본메뉴")

    val oldDf = spark.read.parquet("s3a://sool/HJ/Result/202103/aggResult/parquet/ANVR01.parquet")
    val newDf = spark.read.parquet("s3a://sool/HJ/Result/202103/20211001_paulaner/aggResult/parquet/ANVR01.parquet")

    val oldDfCols = oldDf.columns
    val newDfCols = newDf.columns

    val commonCols = oldDfCols.intersect(newDfCols).map(col)

    val oldDfS = oldDf.select(commonCols:_*)
    val newDfS = newDf.select(commonCols:_*)

    oldDfS.count - newDfS.count

    val oldToNew = oldDfS.except(newDfS)
    val newToOld = newDfS.except(oldDfS)
    oldToNew.count
    newToOld.count
  }

  def checkHjAddrTsv(fileNm: String, ym: String) = {
    val oldDf = spark.read.
      format("csv").
      option("delimiter", "\t").
      option("header", "true").
      load(s"s3a://sool/HJ/Result/${ym}/aggResult/hite${ym}/${fileNm}")

    val newDf = spark.read.
      format("csv").
      option("delimiter", "\t").
      option("header", "true").
      load(s"s3a://sool/HJ/Result/${ym}/20211001_paulaner/aggResult/hite${ym}/${fileNm}")

    // 양수가 나와야 함
    println(newDf.count - oldDf.count)

    val oldToNew = oldDf.except(newDf)
    val newToOld = newDf.except(oldDf)
    println(oldToNew.count())
    println(newToOld.count())
  }

  // HJ 재집계 결과 비교
  def checkHjAnvrTsv(fileNm: String, ym: String) = {
    import org.apache.spark.sql.DataFrame

    def rXlsx(path: String, sheetName: String = "Sheet1"): DataFrame = {
      spark.read.format("com.crealytics.spark.excel").
        option("sheetName", s"$sheetName").
        option("header", "true").
        load(s"${path}")
    }

    val verify = rXlsx("s3a://sool-test/202108/CHANGE_MENU.xlsx", "기본메뉴")

    val oldDf = spark.read.
      format("csv").
      option("delimiter", "\t").
      option("header", "true").
      load(s"s3a://sool/HJ/Result/${ym}/aggResult/hite${ym}/${fileNm}")

    val newDf = spark.read.
      format("csv").
      option("delimiter", "\t").
      option("header", "true").
      load(s"s3a://sool/HJ/Result/${ym}/20211001_paulaner/aggResult/hite${ym}/${fileNm}")

    // 양수가 나와야 함
    println(s"newDf 개수 - oldDf 개수: ${newDf.count - oldDf.count} (양수)")
    println("---------------------------------------------------------------------------------")

    val oldToNew = oldDf.except(newDf)
    val newToOld = newDf.except(oldDf)

    // 1) 277 이하, 2) 280 이 나와야 함
    println(s"oldToNew 메뉴 개수: ${oldToNew.select('NUMER_CD, 'DENOM_CD).distinct.count} (277 이하)")
    println(s"newToOld 메뉴 개수: ${newToOld.select('NUMER_CD, 'DENOM_CD).distinct.count} (280 이하)")
    println("---------------------------------------------------------------------------------")

    // 1) 7 이상, 2) 4 가 나와야 함
    println(s"verify 메뉴들 중에 oldToNew 랑 상이한 메뉴 개수: " +
      s"${verify.join(oldToNew.select('NUMER_CD, 'DENOM_CD).distinct, Seq("NUMER_CD", "DENOM_CD"), "left_anti").count} " +
      s"(7 이상)")
//    verify.join(oldToNew.select('NUMER_CD, 'DENOM_CD).distinct, Seq("NUMER_CD", "DENOM_CD"), "left_anti").
//      select('NUMER_CD, 'DENOM_CD, 'NUMER_DEF, 'DENOM_DEF).distinct.show(false)
    println(s"oldToNew 메뉴들 중에 verify 랑 상이한 메뉴 개수: " +
      s"${oldToNew.select('NUMER_CD, 'DENOM_CD).distinct.join(verify, Seq("NUMER_CD", "DENOM_CD"), "left_anti").count} " +
      s"(4)")
//    oldToNew.select('NUMER_CD, 'DENOM_CD).distinct.join(verify, Seq("NUMER_CD", "DENOM_CD"), "left_anti").show
    println("---------------------------------------------------------------------------------")

    // 1) 280 이하, 2) 0 이 나와야 함
    println(s"verify 메뉴들 중에 newToOld 랑 상이한 메뉴 개수: " +
      s"${verify.join(newToOld.select('NUMER_CD, 'DENOM_CD).distinct, Seq("NUMER_CD", "DENOM_CD"), "left_anti").count} " +
      s"(280 이하)")
//    verify.join(newToOld.select('NUMER_CD, 'DENOM_CD).distinct, Seq("NUMER_CD", "DENOM_CD"), "left_anti").show
    println(s"newToOld 메뉴들 중에 verify 랑 상이한 메뉴 개수: " +
      s"${newToOld.select('NUMER_CD, 'DENOM_CD).distinct.join(verify, Seq("NUMER_CD", "DENOM_CD"), "left_anti").count} " +
      s"(0)")
//    newToOld.select('NUMER_CD, 'DENOM_CD).distinct.join(verify, Seq("NUMER_CD", "DENOM_CD"), "left_anti").show
    println("---------------------------------------------------------------------------------")

    val verifyOldToNew = verify.
      join(oldToNew.select('NUMER_CD, 'DENOM_CD).distinct, Seq("NUMER_CD", "DENOM_CD"), "left_anti").
      filter(!'NUMER_DEF.contains("파울라너에일")).
      filter(!'NUMER_DEF.contains("파울라너라거"))
    println("verify 메뉴들 중에 oldToNew 랑 상이한 메뉴(파울라너에일, 파울라너라거 제외):")
    verifyOldToNew.show(verifyOldToNew.count.toInt, false)
    println("---------------------------------------------------------------------------------")

    val verifyNewToOld = verify.
      join(newToOld.select('NUMER_CD, 'DENOM_CD).distinct, Seq("NUMER_CD", "DENOM_CD"), "left_anti")
    println("verify 메뉴들 중에 newToOld 랑 상이한 메뉴:")
    verifyNewToOld.show(verifyNewToOld.count.toInt, false)
    println("---------------------------------------------------------------------------------")

  }

  def run = {
    checkCnsst2()
  }
}