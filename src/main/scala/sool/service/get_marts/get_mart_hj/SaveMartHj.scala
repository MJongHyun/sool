/**
 * HJ 마트 저장

 */
package sool.service.get_marts.get_mart_hj

import org.apache.spark.sql.DataFrame
import sool.common.path.FilePath
import sool.common.function.FileFunc

class SaveMartHj(spark: org.apache.spark.sql.SparkSession) {
  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  def saveMartHjNonAddr(martHjbNonAddr:DataFrame, martHjsNonAddr:DataFrame, ethDt:String, flag: String) = {
    // write mart_hite_non_addr parquet
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(martHjbNonAddr, filePathCls.hjwMartBExceptAddrPath)
    fileFuncCls.wParquet(martHjsNonAddr, filePathCls.hjwMartSExceptAddrPath)
  }

  def saveMartHj(martHjb:DataFrame, martHjs:DataFrame, ethDt:String, flag: String) = {
    // write mart_hite parquet
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(martHjb, filePathCls.hjwMartBPath)
    fileFuncCls.wParquet(martHjs, filePathCls.hjwMartSPath)
  }
}
