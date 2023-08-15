/**

 */
package sool.address.refine_addr

import org.apache.spark.sql.DataFrame

class RawAddr(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  def apply(dtiEthanol: DataFrame) = {
    val dtiEthSupAddr = dtiEthanol.select('SUP_ADDR.as("COM_ADDR"))
    val dtiEthByrAddr = dtiEthanol.select('BYR_ADDR.as("COM_ADDR"))
    val rawAddr = dtiEthSupAddr.union(dtiEthByrAddr).distinct()
    rawAddr
  }
}
