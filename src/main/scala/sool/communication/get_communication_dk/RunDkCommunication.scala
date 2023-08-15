package sool.communication.get_communication_dk

import sool.common.jdbc
import sool.communication.get_communication_dk._
import sool.communication.run.RunCommunication.logger

class RunDkCommunication (spark: org.apache.spark.sql.SparkSession) {
  def runDkCommunication() = {
    logger.info("[appName=communication] [function=runDkCommunication] [runStatus=start] [message=start]")

    val jdbcGetObj = new jdbc.JdbcGet(spark)
    val getExcelData = new GetDkExcelData(spark)
    val getRenameObj = new GetDkExcelColNm(spark)
    //    위스키
    val dkWskItemRaw = jdbcGetObj.getItemTblConDesc("DK_WSK_ITEM")
    val dkWskExcelData = getExcelData.getDkWskjItemExcelData(dkWskItemRaw)
    val dkWskExcelCol = jdbcGetObj.getItemColCD("DK", "W")
    val dkWskItem = getRenameObj.getDkItemExcelColNm(dkWskExcelData, dkWskExcelCol)

    //    맥주
    val dkBrItemRaw = jdbcGetObj.getItemTblConDesc("DK_BR_ITEM")
    val dkBrExcelData = getExcelData.getDkBrItemExcelData(dkBrItemRaw)
    val dkBrExcelCol = jdbcGetObj.getItemColCD("DK", "B")
    val dkBrItem = getRenameObj.getDkItemExcelColNm(dkBrExcelData, dkBrExcelCol)

    //    메뉴
    val dkMnRaw = jdbcGetObj.getCurrentMnIntr("DK_MN_INTR")
    //    필터
    val dkFltrRaw = jdbcGetObj.getFltrRLTbl("DK_FLTR_INTR")
    val dkMnExcelData = getExcelData.getDkMnExcelData(dkMnRaw, dkFltrRaw)
    val dkMnExcelCol = jdbcGetObj.getMnColCD("DK")
    val dkMn = getRenameObj.getDkMnExcelColNm(dkMnExcelData, dkMnExcelCol)

    //    저장하기

    logger.info("[appName=communication] [function=runDkCommunication] [runStatus=start] [message=start]")
  }
}