/**
 * 코드 전반적으로 사용되는 함수들 모음

 */
package sool.common.function

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

class CmnFunc extends java.io.Serializable {
  // spark
  def getSpark() = {
    val spark: SparkSession = SparkSession.
      builder.
      appName("sool").
      getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "") // sool 서버에 있던 key
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "") // sool 서버에 있던 key
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") //  로컬에 필요 했었음
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") //  로컬에 필요 했었음
    spark.sparkContext.setLogLevel("WARN") // Spark Context 로그 레벨 설정
    spark
  }

  // logger
  def getLogger() = {
    val logger = LogManager.getLogger("myLogger")
    logger.setLevel(Level.INFO) // "myLogger" 에 해당하는 로그 레벨 설정
    logger
  }
}
