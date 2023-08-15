/**
 * 코드 전반적으로 사용되는 파일 읽고 쓰는 함수들 모음

 */
package sool.common.function

import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import sool.service.run.RunService.logger

class FileFunc(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  // 필요 클래스 선언
  val getTimeCls = new GetTime()

  // read parquet
  def rParquet(path: String) = spark.read.parquet(path)

  // write parquet
  def wParquet(df: DataFrame, path: String) = {
    logger.info(s"[file=FileFunc] [function=wParquet] [status=start] [message=${path} 에 저장을 시작합니다.]")
    df.coalesce(1).write.parquet(path)
    logger.info(s"[file=FileFunc] [function=wParquet] [status=end] [message=${path} 에 저장되었습니다.]")
  }

  // overwrite parquet
  def owParquet(df: DataFrame, path: String) = {
    logger.info(s"[file=FileFunc] [function=owParquet] [status=start] [message=${path} 에 저장을 시작합니다. (overwrite)]")
    df.coalesce(1).write.mode("overwrite").parquet(path)
    logger.info(s"[file=FileFunc] [function=owParquet] [status=end] [message=${path} 에 저장되었습니다. (overwrite)]")
  }

  // read csv
  def rCsv(path : String, hdrOpt: String): DataFrame = {
    spark.read.format("csv").option("header", hdrOpt).load(s"${path}")
  }

  // write csv
  def wCsv(df: org.apache.spark.sql.DataFrame, path: String, hdrOpt: String) = {
    logger.info(s"[file=FileFunc] [function=wCsv] [status=start] [message=${path} 에 저장을 시작합니다.]")
    df.repartition(1).write.
      format("com.databricks.spark.csv").
      option("header", hdrOpt).
      save(s"${path}")
    logger.info(s"[file=FileFunc] [function=wCsv] [status=end] [message=${path} 에 저장되었습니다.]")
  }

  /*
   tsv 파일 쓰기 (2021.07.27 기준 HJ 결과 생성을 위해 사용)
   기존 술 서버 spark 2.3.0 에서 생성하는 tsv 랑 spark 2.4.7(현재) 에서 생성하는 tsv 결과가 달라서
   option("emptyValue", null) 을 넣어야 함
   */
  def wTsv(df: org.apache.spark.sql.DataFrame, path: String) = {
    logger.info(s"[file=FileFunc] [function=wTsv] [status=start] [message=${path} 에 저장을 시작합니다.]")
    df.repartition(1).write.
      option("header", "true").
      option("delimiter", "\t").
      option("encoding", "UTF-8").
      option("emptyValue", null).
      csv(s"${path}")
    logger.info(s"[file=FileFunc] [function=wTsv] [status=end] [message=${path} 에 저장되었습니다.]")
  }

  // tsv 파일 읽기
  def rTsv(path: String) = {
    val res = spark.read.
      format("csv").
      option("delimiter", "\t").
      option("header", "true").
      load(path)
    res
  }

  // 엑셀 read
  def rXlsx(path: String, sheetName: String = "Sheet1"): DataFrame = {
    spark.read.format("com.crealytics.spark.excel").
      option("sheetName", s"$sheetName").
      option("header", "true").
      load(s"${path}")
  }

  // 엑셀 생성
  def wXlsx(df: DataFrame, path:String) = {
    logger.info(s"[file=FileFunc] [function=wXlsx] [status=start] [message=${path} 에 저장을 시작합니다.]")
    df.write.
      format("com.crealytics.spark.excel").
      option("sheetName", "1nmInfo").
    //option("useHeader", "true").
      option("header", "true").
      // option("addColorColumns", "true").
      save(s"${path}")
    logger.info(s"[file=FileFunc] [function=wXlsx] [status=end] [message=${path} 에 저장되었습니다.]")
  }

  // 여러 시트 생성 엑셀
  def multiSheetXlsx(df: DataFrame, sheetNm: String, path: String) = {
    logger.info(s"[file=FileFunc] [function=multiSheetXlsx] [status=start] [message=${path} 에 저장을 시작합니다.]")
    df.coalesce(1).write.
      format("com.crealytics.spark.excel").
      option("dataAddress", s"'${sheetNm}'!A1:AZ100000").
      option("header", "true").   // Zeppelin 에서는 옵션명이 "useHeader" 였음
      mode("append").
      save(s"${path}")
    logger.info(s"[file=FileFunc] [function=multiSheetXlsx] [status=end] [message=${path} 에 저장되었습니다.]")
  }

  /*
   delimiter 사용하는 write csv
   HJ 결과 생성할 때 사용했었는데 spark 버전 차이로 기존 제공하던 결과랑 달라서 wTsv 라는 걸 만들어서 사용하기로 한다.
   추후 tab 말고 다른 delimiter 를 사용하게 될 때 사용할 함수
   */
  def wCsvDlm(df: DataFrame, path: String, dlm: String) = {
    logger.info(s"[file=FileFunc] [function=wCsvDlm] [status=start] [message=${path} 에 저장을 시작합니다.]")
    df.
      repartition(1).
      write.
      //      mode("overwrite").
      option("header", "true").
      option("delimiter", dlm).
      option("encoding", "UTF-8").
      csv(path)
    logger.info(s"[file=FileFunc] [function=wCsvDlm] [status=end] [message=${path} 에 저장되었습니다.]")
  }

  // s3a 파일 존재 여부 체크
  def checkS3FileExist(path: String) = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(new URI("s3a://sool"), conf)
    val res = fs.exists(new Path(path))
    logger.info(s"[file=FileFunc] [function=checkS3FileExist] [status=running] [message=${path} 파일 존재 여부: ${res}]")
    res
  }
}