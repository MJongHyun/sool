/**
 * HK 마트 저장

 */
package sool.service.get_marts.get_mart_hk

import org.apache.spark.sql.DataFrame
import sool.common.path.FilePath
import sool.common.function.FileFunc

class SaveMartHk(spark: org.apache.spark.sql.SparkSession) {
  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  def saveMartHk(martHk:DataFrame, ethDt:String, flag:String) = {
    // write mart_hnk parquet (기존 파일 생성명이 martHK 인데 martHNK 로 할 건지 mart_hnk 로 할 건지 결정해야 함)
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(martHk, filePathCls.hkwMartPath)
  }
}