/**
 * DK(DK) 마트 저장

 */
package sool.service.get_marts.get_mart_dk

import org.apache.spark.sql.DataFrame
import sool.common.path.FilePath
import sool.common.function.FileFunc

class SaveMartDk(spark: org.apache.spark.sql.SparkSession) {
  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  // write mart_dk parquet
  def saveMartDk(martDkb:DataFrame, martDkw:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(martDkb, filePathCls.dkwMartBPath)
    fileFuncCls.wParquet(martDkw, filePathCls.dkwMartWPath)
  }
}
