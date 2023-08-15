/**

 */
package sool.extract.extract_data

import org.apache.spark.sql.DataFrame
import sool.common.function.FileFunc
import sool.common.path.FilePath

class ExtrctDtiRetail(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  // 도매상 매입 거래 제거
  def getDtiRetail(dtiEthanol: DataFrame, vendor: DataFrame) = {
    val vendorByr = vendor.select('RGNO.as("BYR_RGNO"))
    val dtiEthByr = dtiEthanol.select('BYR_RGNO).distinct
    val dtiEthByrExceptVendor = dtiEthByr.except(vendorByr)
    val dtiRetail = dtiEthanol.join(dtiEthByrExceptVendor, Seq("BYR_RGNO"))
    dtiRetail
  }

  // 메인
  def extrctDtiRetail(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)

    /* 도매상 매입 거래 제거 (** 공급거래 존재: 도매상으로 판단 **) */
    val dtiEthanol = fileFuncCls.rParquet(filePathCls.dtiEthPath)
    val vendor = fileFuncCls.rParquet(filePathCls.vendorPath)
    val dtiRetail = getDtiRetail(dtiEthanol, vendor)
    fileFuncCls.wParquet(dtiRetail, filePathCls.dtiRtlPath) // 도매상 매입 거래 제거한 데이터 저장
  }
}
