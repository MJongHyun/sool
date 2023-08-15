/**
 * HK 집계 데이터 준비

 */
package sool.service.aggregate.agg_hk

import sool.common.path.FilePath
import sool.common.jdbc.JdbcGet

class GetAggDfsHk(spark: org.apache.spark.sql.SparkSession) {
  val jdbcGetCls = new JdbcGet(spark)

  def getAggDfsHk(ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    // mart_hk
    val martHk = spark.read.parquet(filePathCls.hkrMartPath)

    // DB, HK 마스터
    val mstrHk = jdbcGetCls.getHkAgrnItemTbl("HK_AGRN_ITEM")

    // branch_Done (Branch : Seoul, CHN, ...)
    val branchDone = spark.read.parquet(filePathCls.hkBranchDonePath)
    (martHk, mstrHk, branchDone)
  }
}
