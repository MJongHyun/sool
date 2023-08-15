package sool.service.aggregate.agg_hj3

import org.apache.spark.sql.functions.col
import sool.address.run.RunBldngMgmtNmbAddr2.fileFuncCls
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath
import sool.service.aggregate.agg_hj2.{GetAggDfsHj2, GetAnlysMartHj2, GetComRgnoHj2, GetResHj2, SaveDfsHj2}
import sool.service.run.RunService.logger
import org.apache.spark.sql.functions.{when, lit}

class RunAggHj3(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._
  val jdbcGet = new JdbcGet(spark)

  // 집계에 필요한 클래스 선언
  val getAggDfsHj2Cls = new GetAggDfsHj2(spark)
  val getComRgnoHj2Cls = new GetComRgnoHj2(spark)
  val getAnlysMartHj2Cls = new GetAnlysMartHj2(spark)
  val getResHj2Cls = new GetResHj2(spark)
  val saveDfsHj2Cls = new SaveDfsHj2(spark)
  val hj3Func = new GetHj3Func(spark)

  def runAggHj3Busan(ethDt:String, flag:String) = {
    val filePathCls = new FilePath(ethDt, flag)
    logger.info(s"[file=runAggHj3Busan] [function=runAggHj3] [status=running] [message=부산지역 소주 MS 추출 시작]")
    // 부산광역시 데이터
    val martHiteSojuBusan = fileFuncCls.rParquet(filePathCls.hjwMartSPath).filter($"ADDR1" === "부산광역시")
    val hj3ItemMaster = jdbcGet.getItemTbl("HJ3_ITEM").filter(col("TYPE_CD")==="S")

    val busanBrandData = martHiteSojuBusan.join(hj3ItemMaster, Seq("ITEM")).
      withColumnRenamed("ADDR0", "LEVEL0").
      withColumnRenamed("ADDR1", "LEVEL1").
      withColumnRenamed("ADDR2", "LEVEL2").
      withColumnRenamed("ADDR3D", "LEVEL3").
      withColumnRenamed("ADDR3R", "LEVEL4")

    // level별 분모/분자값 추출
    val (denoLvl1, denoLvl2, denoLvl3, denoLvl4, denoLvl5, denoLvl6) = hj3Func.getDenoLvlG(busanBrandData)
    val (numeLvl1, numeLvl2, numeLvl3, numeLvl4, numeLvl5, numeLvl6) = hj3Func.getNumeLvlG(busanBrandData)

    // level별 select, join 컬럼 함수 불러오기
    val lvlColsMap = hj3Func.getLvlColsMap()
    val lvlJoinColsMap = hj3Func.getLvlJoinColsMap()

    // level별 파일 추출
    val anvrLv1 = hj3Func.makeResultLevelData(numeLvl1, denoLvl1, lvlColsMap("lvl1"), lvlJoinColsMap("lvl1"), ethDt, "ANVR01")
    val anvrLv2 = hj3Func.makeResultLevelData(numeLvl2, denoLvl2, lvlColsMap("lvl2"), lvlJoinColsMap("lvl2"), ethDt, "ANVR02")
    val anvrLv3 = hj3Func.makeResultLevelData(numeLvl3, denoLvl3, lvlColsMap("lvl3"), lvlJoinColsMap("lvl3"), ethDt, "ANVR03")
    val anvrLv4 = hj3Func.makeResultLevelData(numeLvl4, denoLvl4, lvlColsMap("lvl4"), lvlJoinColsMap("lvl4"), ethDt, "ANVR04")
    val anvrLv5 = hj3Func.makeResultLevelData(numeLvl5, denoLvl5, lvlColsMap("lvl5"), lvlJoinColsMap("lvl5"), ethDt, "ANVR05")
    val anvrLv6 = hj3Func.makeResultLevelData(numeLvl6, denoLvl6, lvlColsMap("lvl6"), lvlJoinColsMap("lvl6"), ethDt, "ANVR06")

    // 추출한 데이터 하나의 데이터로 통합
    val anvrLv1DF = hj3Func.makeFinalDF(anvrLv1, anvrLv6)
    val anvrLv2DF = hj3Func.makeFinalDF(anvrLv2, anvrLv6)
    val anvrLv3DF = hj3Func.makeFinalDF(anvrLv3, anvrLv6)
    val anvrLv4DF = hj3Func.makeFinalDF(anvrLv4, anvrLv6)
    val anvrLv5DF = hj3Func.makeFinalDF(anvrLv5, anvrLv6)
    val anvrLv6DF = hj3Func.makeFinalDF(anvrLv6, anvrLv6)

    val anvrTotal = anvrLv1DF.union(anvrLv2DF).union(anvrLv3DF).union(anvrLv4DF).union(anvrLv5DF).union(anvrLv6DF)
    // 저장
    fileFuncCls.wParquet(anvrTotal, filePathCls.hj3BusanParquetPath)
    val anvrTotalDf = fileFuncCls.rParquet(filePathCls.hj3BusanParquetPath)
    fileFuncCls.wXlsx(anvrTotalDf, filePathCls.hj3BusanXlsxPath)
    logger.info(s"[file=runAggHj3Busan] [function=runAggHj3] [status=running] [message=부산지역 소주 MS 추출 완료]")
  }

  def runAggHj3SudoGwon(ethDt:String, flag:String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val sidoList = List("서울특별시", "인천광역시", "경기도")
    logger.info(s"[file=runAggHj3SudoGwon [function=runAggHj3] [status=running] [message=수도권 맥주 MS 추출 시작]")
    val sudoData = fileFuncCls.rParquet(filePathCls.hjwMartBPath).filter($"ADDR1".isin(sidoList:_*))
    val hj3ItemMaster = jdbcGet.getItemTbl("HJ3_ITEM").filter(col("TYPE_CD")==="B")

    val sudoBrandData = sudoData.join(hj3ItemMaster, Seq("ITEM")).
      withColumnRenamed("ADDR0", "LEVEL0").
      withColumnRenamed("ADDR1", "LEVEL1").
      withColumnRenamed("ADDR2", "LEVEL2").
      withColumnRenamed("ADDR3D", "LEVEL3").
      withColumnRenamed("ADDR3R", "LEVEL4")
    val (denoLvl0 ,denoLvl1, denoLvl2, denoLvl3, denoLvl4, denoLvl5, denoLvl6) = hj3Func.getDenoLvlG0(sudoBrandData)
    val (numeLvl0, numeLvl1, numeLvl2, numeLvl3, numeLvl4, numeLvl5, numeLvl6) = hj3Func.getNumeLvlG0(sudoBrandData)
    // level별 select, join 컬럼 함수 불러오기
    val lvlColsMap = hj3Func.getLvlColsMap()
    val lvlJoinColsMap = hj3Func.getLvlJoinColsMap()
    // level별 파일 추출
    val anvrLv0pre = hj3Func.makeResultLevelData(numeLvl0, denoLvl0, lvlColsMap("lvl0"), lvlJoinColsMap("lvl0"), ethDt, "ANVR01")
    val anvrLv1pre = hj3Func.makeResultLevelData(numeLvl1, denoLvl1, lvlColsMap("lvl1"), lvlJoinColsMap("lvl1"), ethDt, "ANVR01")
    val anvrLv1 = anvrLv0pre.union(anvrLv1pre).withColumnRenamed("LEVEL0", "LEVEL1")
    val anvrLv2 = hj3Func.makeResultLevelData(numeLvl2, denoLvl2, lvlColsMap("lvl2"), lvlJoinColsMap("lvl2"), ethDt, "ANVR02")
    val anvrLv3 = hj3Func.makeResultLevelData(numeLvl3, denoLvl3, lvlColsMap("lvl3"), lvlJoinColsMap("lvl3"), ethDt, "ANVR03")
    val anvrLv4 = hj3Func.makeResultLevelData(numeLvl4, denoLvl4, lvlColsMap("lvl4"), lvlJoinColsMap("lvl4"), ethDt, "ANVR04")
    val anvrLv5 = hj3Func.makeResultLevelData(numeLvl5, denoLvl5, lvlColsMap("lvl5"), lvlJoinColsMap("lvl5"), ethDt, "ANVR05")
    val anvrLv6 = hj3Func.makeResultLevelData(numeLvl6, denoLvl6, lvlColsMap("lvl6"), lvlJoinColsMap("lvl6"), ethDt, "ANVR06")
    // 추출한 데이터 하나의 데이터로 통합
    val anvrLv1DF = hj3Func.makeFinalDF(anvrLv1, anvrLv6)
    val anvrLv2DF = hj3Func.makeFinalDF(anvrLv2, anvrLv6)
    val anvrLv3DF = hj3Func.makeFinalDF(anvrLv3, anvrLv6)
    val anvrLv4DF = hj3Func.makeFinalDF(anvrLv4, anvrLv6)
    val anvrLv5DF = hj3Func.makeFinalDF(anvrLv5, anvrLv6)
    val anvrLv6DF = hj3Func.makeFinalDF(anvrLv6, anvrLv6)

    val anvrTotal = anvrLv1DF.union(anvrLv2DF).union(anvrLv3DF).union(anvrLv4DF).union(anvrLv5DF).union(anvrLv6DF)
    // 저장
    fileFuncCls.wParquet(anvrTotal, filePathCls.hj3SudoGwonParquetPath)
    val anvrTotalDF = fileFuncCls.rParquet(filePathCls.hj3SudoGwonParquetPath).
      orderBy("ANVR_SET_CD", "LEVEL1", "LEVEL2", "POST_CD", "LEVEL3", "LEVEL4").
      withColumn("LEVEL1", when($"LEVEL1" === "0전국", lit("전국")).otherwise($"LEVEL1"))
    fileFuncCls.wXlsx(anvrTotalDF, filePathCls.hj3SudoGwonXlsxPath)
    logger.info(s"[file=runAggHj3SudoGwon [function=runAggHj3] [status=running] [message=수도권 맥주 MS 추출 완료]")
  }
  // main
  def runAggHj3(ethDt:String, flag:String) = {
    logger.info("[appName=sool] [function=runAggHj3] [runStatus=start] [message=start]")
    runAggHj3Busan(ethDt, flag)
    runAggHj3SudoGwon(ethDt, flag)
    logger.info("[appName=sool] [function=runAggHj3] [runStatus=end] [message=end]")
  }
}
