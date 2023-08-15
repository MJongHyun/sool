/**
 * 도매상 리스트 업데이트

 */
package sool.extract.extract_data

import org.apache.spark.sql.DataFrame
import sool.common.function.{FileFunc, GetTime}
import sool.common.path.FilePath

class ExtrctVendor(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()

  // vendor_Raw 파일 생성
  def getVendor(dtiEthanol: DataFrame, vendorBf1m:DataFrame) = {
    val dtiEthSup = dtiEthanol.select('SUP_RGNO.as("RGNO")).distinct
    val vendor = vendorBf1m.union(dtiEthSup).distinct
    vendor
  }

  // 메인
  def extrctVendor(ethDt: String, flag: String) = {
    val ethBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)

    /* 도매상 리스트 업데이트 (공급 내역 존재 업체 도매상 리스트에 추가) */
    val dtiEthanol = fileFuncCls.rParquet(filePathCls.dtiEthPath)
    val vendorBf1m = fileFuncCls.rParquet(filePathClsBf1m.vendorPath)
    val vendor = getVendor(dtiEthanol, vendorBf1m)
    fileFuncCls.wParquet(vendor, filePathCls.vendorPath)  // 집계연월의 vendor.parquet 저장
  }
}