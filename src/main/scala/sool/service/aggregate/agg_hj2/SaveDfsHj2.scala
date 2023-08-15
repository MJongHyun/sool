/**
 * HJ 2차 집계 결과들 파일로 저장

 */
package sool.service.aggregate.agg_hj2

import org.apache.spark.sql.DataFrame
import sool.common.path.FilePath
import sool.common.function.FileFunc

class SaveDfsHj2(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  // HJ 2차 40000 개 업체 리스트 저장
  def saveComRgnoHj2(comRgnoHj2:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(comRgnoHj2, filePathCls.hj2ComRgnoPath)
  }

  // HJ 2차 분석 마트 저장
  def saveAnlysMartHj2(anlysMartHj2:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(anlysMartHj2, filePathCls.hj2AnlysMartPath)
  }

  // HJ 2차 업체 정보 및 분석 결과 parquet 저장
  def saveResHj2Parquet(comResHj2:DataFrame, anlysResHj2:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    fileFuncCls.wParquet(comResHj2, filePathCls.hj2ComResPath)
    fileFuncCls.wParquet(anlysResHj2, filePathCls.hj2AnlysResPath)
  }

  // HJ 2차 업체 정보 및 분석 결과 tsv 저장
  def saveResHj2Tsv(comResHj2:DataFrame, anlysResHj2:DataFrame, ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)

    val comResHj2Ord = comResHj2.orderBy('VAT_NO.asc)
    fileFuncCls.wTsv(comResHj2Ord, filePathCls.hj2ComResTsvPath)
    val anlysResHj2Ord = anlysResHj2.orderBy('VAT_NO.asc, 'MARKET_CODE.asc)
    fileFuncCls.wTsv(anlysResHj2Ord, filePathCls.hj2AnlysResTsvPath)
  }
}
