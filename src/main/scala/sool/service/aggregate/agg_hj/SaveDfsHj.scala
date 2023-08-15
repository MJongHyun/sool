/**

 */
package sool.service.aggregate.agg_hj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import sool.common.function.FileFunc
import sool.common.path.FilePath

class SaveDfsHj(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  // ANVR 결과 저장
  def saveAnvrDfs(ethDt:String,
                  anvr01:DataFrame,
                  anvr02:DataFrame,
                  anvr03:DataFrame,
                  anvr04:DataFrame,
                  anvr05:DataFrame,
                  anvr06:DataFrame,
                  flag:String) = {
    val filePathCls = new FilePath(ethDt, flag)
    if (flag == "inv") {
      val newEthDt = ethDt.toInt+700000
      fileFuncCls.wParquet(anvr01.withColumn("YM", lit(newEthDt)), filePathCls.hjAnvr01Path)
      fileFuncCls.wParquet(anvr02.withColumn("YM", lit(newEthDt)), filePathCls.hjAnvr02Path)
      fileFuncCls.wParquet(anvr03.withColumn("YM", lit(newEthDt)), filePathCls.hjAnvr03Path)
      fileFuncCls.wParquet(anvr04.withColumn("YM", lit(newEthDt)), filePathCls.hjAnvr04Path)
      fileFuncCls.wParquet(anvr05.withColumn("YM", lit(newEthDt)), filePathCls.hjAnvr05Path)
      fileFuncCls.wParquet(anvr06.withColumn("YM", lit(newEthDt)), filePathCls.hjAnvr06Path)
    }
    else{
      fileFuncCls.wParquet(anvr01, filePathCls.hjAnvr01Path)
      fileFuncCls.wParquet(anvr02, filePathCls.hjAnvr02Path)
      fileFuncCls.wParquet(anvr03, filePathCls.hjAnvr03Path)
      fileFuncCls.wParquet(anvr04, filePathCls.hjAnvr04Path)
      fileFuncCls.wParquet(anvr05, filePathCls.hjAnvr05Path)
      fileFuncCls.wParquet(anvr06, filePathCls.hjAnvr06Path)
    }
  }

  // TSV 정의 및 코드 테이블 저장 {CD_DIMENSION, CD_ANVR, LV2W_MAP}
  def writeCdTbls(ethDt: String, cdTbl: DataFrame, cdTblNm: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    if (flag=="inv"){
      val newEthDt = ethDt.toInt+700000
      val cdTblAddCol = cdTbl.
        withColumn("YM", lit(newEthDt))
      val cdTblCols = (Seq("YM") ++ cdTbl.columns.filter(_ != "YM")).map(col)
      val cdTblTsv = cdTblAddCol.select(cdTblCols:_*)
      val tsvSavePath = filePathCls.hjTsvSavePath + s"${cdTblNm}.tsv" // 저장 경로

      fileFuncCls.wTsv(cdTblTsv, tsvSavePath)
    }
    else{
      val cdTblAddCol = cdTbl.
        withColumn("YM", lit(ethDt))
      val cdTblCols = (Seq("YM") ++ cdTbl.columns.filter(_ != "YM")).map(col)
      val cdTblTsv = cdTblAddCol.select(cdTblCols:_*)
      val tsvSavePath = filePathCls.hjTsvSavePath + s"${cdTblNm}.tsv" // 저장 경로

      fileFuncCls.wTsv(cdTblTsv, tsvSavePath)
    }

  }

  // CD_DIMENSION 저장 실행
  // 기존 고객사 전송용 CD_DIMENSION 과 정합성 체크 결과 NUMER_DEF 에서 다른 점이 발견됨.
  def saveCdDimension(ethDt:String, hjMnIntr:DataFrame, hjFltrIntr:DataFrame, flag: String) = {
    // 아래 주석은 기존 Zeppelin 에 있던 주석
    // 1~5월: 576개, 6월: 578개(일품진로1924), 7~9월: 598개(필후), 10월~: 600개(자두에이슬)
    val hjMnIntrSlct = hjMnIntr.select(
      'MN_NM.as("MENU_NM"),
      'FLTR_ID1.as("DENOM_CD"),
      'FLTR_ID2.as("NUMER_CD")
    )
    val hjFltrIntrDenoSlct = hjFltrIntr.select(
      'FLTR_ID.as("DENOM_CD"),
      'FLTR_DESC_TXT.as("DENOM_NM"),
      'DESC_TXT.as("DENOM_DEF"))

    val hjFltrIntrNumeSlct = hjFltrIntr.select(
      'FLTR_ID.as("NUMER_CD"),
      'FLTR_DESC_TXT.as("NUMER_NM"),
      'DESC_TXT.as("NUMER_DEF"))

    val cdDimensionDeno = hjMnIntrSlct.join(hjFltrIntrDenoSlct, Seq("DENOM_CD"))
    val cdDimensionDenoNume = cdDimensionDeno.join(hjFltrIntrNumeSlct, Seq("NUMER_CD"))

    // 컬럼 순서 변경 후 최종 cdDimension 생성
    val cdDimension = cdDimensionDenoNume.select(
      "MENU_NM", "NUMER_NM", "NUMER_CD",
      "NUMER_DEF", "DENOM_NM", "DENOM_CD", "DENOM_DEF"
    )

    writeCdTbls(ethDt, cdDimension, "CD_DIMENSION", flag)
  }

  // CD_ANVR 저장 실행
  def saveCdAnvr(ethDt:String, cdAnvrParquet: DataFrame, flag: String)= {
    val cdAnvrParquetSlct = cdAnvrParquet.select("ANVR_SET_CD", "ANVR_SET_NO", "ANVR_SET_DEF")
    val cdAnvr13 = Seq(("ANVR13", "13", "Level2W")).toDF("ANVR_SET_CD", "ANVR_SET_NO", "ANVR_SET_DEF")
    val cdAnvr = cdAnvrParquetSlct.union(cdAnvr13)

    writeCdTbls(ethDt, cdAnvr, "CD_ANVR", flag)
  }

  // 각 파일별 컬럼명 및 컬럼순서 Map
  def getSlctColsMap() = {
    val slctColsMap = Map(
      "ANVR01" -> Seq("YM", "ANVR_SET_CD", "LEVEL1", "NUMER_CD", "DENOM_CD", "QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS", "YN"),
      "ANVR02" -> Seq("YM", "ANVR_SET_CD", "LEVEL1", "LEVEL2", "NUMER_CD", "DENOM_CD", "QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS", "YN"),
      "ANVR03" -> Seq("YM", "ANVR_SET_CD", "LEVEL1", "LEVEL2", "LEVEL3", "NUMER_CD", "DENOM_CD", "QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS", "YN"),
      "ANVR04" -> Seq("YM", "ANVR_SET_CD", "LEVEL1", "LEVEL2", "LEVEL3", "LEVEL4", "NUMER_CD", "DENOM_CD", "QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS", "YN"),
      "ANVR05" -> Seq("YM", "ANVR_SET_CD", "LEVEL1", "LEVEL2", "POST_CD", "NUMER_CD", "DENOM_CD", "QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS", "YN"),
      "ANVR06" -> Seq("YM", "ANVR_SET_CD", "LEVEL1", "LEVEL2", "POST_CD", "LEVEL3", "LEVEL4", "NUMER_CD", "DENOM_CD", "QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS", "YN"),
      "ANVR13" -> Seq("YM", "ANVR_SET_CD", "LEVEL2W", "NUMER_CD", "DENOM_CD", "QT_MARKET", "QT_MS", "AMT_MARKET", "AMT_MS", "COM_MARKET", "COM_MS", "YN"),
      "ADDR01" -> Seq("YM", "LEVEL1"),
      "ADDR02" -> Seq("YM", "LEVEL1", "LEVEL2"),
      "ADDR03" -> Seq("YM", "LEVEL1", "LEVEL2", "LEVEL3"),
      "ADDR04" -> Seq("YM", "LEVEL1", "LEVEL2", "LEVEL3", "LEVEL4"),
      "ADDR05" -> Seq("YM", "LEVEL1", "LEVEL2", "POST_CD"),
      "ADDR06" -> Seq("YM", "LEVEL1", "LEVEL2", "POST_CD", "LEVEL3", "LEVEL4"),
      "CD_DIMENSION" -> Seq("YM", "MENU_NM", "NUMER_NM", "NUMER_CD", "NUMER_DEF", "DENOM_NM", "DENOM_CD", "DENOM_DEF"),
      "CD_ANVR" -> Seq("YM", "ANVR_SET_CD", "ANVR_SET_NO", "ANVR_SET_DEF"),
      "LV2W_MAP" -> Seq("YM", "LEVEL1", "LEVEL2", "LEVEL2W")
    )
    slctColsMap
  }

  // ANVR01 ~ ANVR06 및 ADDR01 ~ ADDR06 을 TSV 로 저장 실행 하는 함수
  def saveAnvrAddrTsv(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)

    val slctColsMap = getSlctColsMap()  // 파일별 컬럼명 Map
    val anvrFileNmList = Seq("ANVR01", "ANVR02", "ANVR03", "ANVR04", "ANVR05", "ANVR06")  // 파일명 리스트

    // 파일별 저장
    anvrFileNmList.foreach(anvrFileNm => {
      val slctColsList = slctColsMap(anvrFileNm)  // 컬럼 리스트

      // ANVR parquet 파일 로드
      val anvrPath = filePathCls.hjAnvrSavePath + s"${anvrFileNm}.parquet"
      val anvrDf = spark.read.parquet(anvrPath)
      val anvrSlctCols = slctColsList.map(col) // filterNot(_=="YN").
      val anvrDfSlct = anvrDf.select(anvrSlctCols: _*)
      val saveAnvrTsvPath = filePathCls.hjTsvSavePath + s"${anvrFileNm}.tsv"

      val newEthDt = ethDt.toInt+700000
      if (flag=="inv") {
        // ANVR01 ~ ANVR06 TSV 로 저장
        fileFuncCls.wTsv(anvrDfSlct.withColumn("YM", lit(newEthDt)), saveAnvrTsvPath)
      }
      else{
        // ANVR01 ~ ANVR06 TSV 로 저장
        fileFuncCls.wTsv(anvrDfSlct, saveAnvrTsvPath)
      }
      val lvlColsList = List("LEVEL1", "LEVEL2", "POST_CD", "LEVEL3", "LEVEL4")
      val addrCols = anvrDf.columns.filter(lvlColsList.contains(_))
      val addrSlctCols = (Seq("YM") ++ addrCols).map(col)
      val addrDf = anvrDf.select(addrSlctCols:_*).distinct.orderBy(addrCols.map(col):_*)
      val saveAddrTsvNm = anvrFileNm.replace("ANVR", "ADDR") + ".tsv"
      val saveAddrTsvPath = filePathCls.hjTsvSavePath + s"${saveAddrTsvNm}"

      if (flag=="inv"){
        fileFuncCls.wTsv(addrDf.withColumn("YM", lit(newEthDt)), saveAddrTsvPath)
      }
      else{
        // ADDR01 ~ ADDR06 TSV 로 저장
        fileFuncCls.wTsv(addrDf, saveAddrTsvPath)
      }
    })
  }

  // 공급업체 기준 맥주, 소주 마트 저장
  def saveHjSupMart(ethDt: String, hjbSupMart: DataFrame, hjsSupMart: DataFrame, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(hjbSupMart, filePathCls.hjSupMartBPath)
    fileFuncCls.wParquet(hjsSupMart, filePathCls.hjSupMartSPath)
  }

  // 공급업체 기준 맥주 + 소주 통합 분모, 분자 마트 저장
  def saveHjSupDenoNume(ethDt: String, hjSupDenoNume: DataFrame, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(hjSupDenoNume, filePathCls.hjSupDenoNumePath)
  }

  // ANVR13 저장
  def saveAnvr13Tsv(ethDt: String, anvr13: DataFrame, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    if (flag=="inv"){
      val newEthDt = ethDt.toInt+700000
      fileFuncCls.wTsv(anvr13.withColumn("YM", lit(newEthDt)), filePathCls.hjAnvr13Path)
    }
    else{
      fileFuncCls.wTsv(anvr13, filePathCls.hjAnvr13Path)
    }
  }
}
