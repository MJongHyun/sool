/**
 * HK 집계 결과들 파일로 저장

 */
package sool.service.aggregate.agg_hk

import org.apache.spark.sql.DataFrame
import sool.common.path.FilePath
import sool.common.function.FileFunc

class SaveDfsHk(spark: org.apache.spark.sql.SparkSession) {
  // 필요한 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  def saveDenoDfHk(denoDfHk:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(denoDfHk, filePathCls.hkDenoPath)
  }

  def saveNumeDfHk(numeDfHk:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(numeDfHk, filePathCls.hkNumePath)
  }

  def saveResHk(resHk:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(resHk, filePathCls.hkResPath)
  }

  def saveResHkXlsx(mktTot:DataFrame, mktDra:DataFrame, mktNor:DataFrame, mktOth:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.multiSheetXlsx(mktTot, "Total", filePathCls.hkMrtPath)
    fileFuncCls.multiSheetXlsx(mktDra, "Draught", filePathCls.hkMrtPath)
    fileFuncCls.multiSheetXlsx(mktNor, "Normal", filePathCls.hkMrtPath)
    fileFuncCls.multiSheetXlsx(mktOth, "Others", filePathCls.hkMrtPath)
  }
}
