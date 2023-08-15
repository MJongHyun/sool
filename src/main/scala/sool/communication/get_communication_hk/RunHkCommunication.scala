package sool.communication.get_communication_hk

import sool.common.jdbc
import sool.communication.run.RunCommunication.logger

class RunHkCommunication (spark: org.apache.spark.sql.SparkSession) {
  def runHkCommunication() = {
    logger.info("[appName=communication] [function=runHkCommunication] [runStatus=start] [message=start]")

    val jdbcGetObj = new jdbc.JdbcGet(spark)

    val hkItemRaw = jdbcGetObj.getItemTblConDesc("HK_AGRN_ITEM")
    val hkExcelCol = jdbcGetObj.getItemColCD("HK", "A")
    val hkItem = new GetHkExcelColNm(spark).getHkExcelColNm(hkItemRaw, hkExcelCol)

    //    todo
    //    저장하기

    logger.info("[appName=communication] [function=runHkCommunication] [runStatus=start] [message=start]")
  }
}