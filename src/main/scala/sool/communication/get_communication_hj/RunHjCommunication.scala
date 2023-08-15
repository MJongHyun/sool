package sool.communication.get_communication_hj

import sool.common.jdbc
import sool.communication.run.RunCommunication.logger

class RunHjCommunication (spark: org.apache.spark.sql.SparkSession) {
  def runHjCommunication() = {
    logger.info("[appName=communication] [function=runHjCommunication] [runStatus=start] [message=start]")
    val jdbcGetObj = new jdbc.JdbcGet(spark)
    val getExcelData = new GetHjExcelData(spark)
    val getRenameObj = new GetHjExcelColNm(spark)

//    소주
//    소주 아이템 데이터 가져오기
    val hjSjItemRaw = jdbcGetObj.getItemTblConDesc("HJ_SJ_ITEM")
//    엑셀에 맞는 데이터 추출
    val hjSjExcelData = getExcelData.getHjSjItemExcelData(hjSjItemRaw)
//    컬럼매핑 가져오기
    val hjSjExcelCol = jdbcGetObj.getItemColCD("HJ", "S")
//    엑셀에 맞는 컬럼으로 바꾸기
    val hjSjItem = getRenameObj.getHjItemExcelColNm(hjSjExcelData, hjSjExcelCol)

//    맥주
//    맥주 아이템 데이터 가져오기
    val hjBrItemRaw = jdbcGetObj.getItemTblConDesc("HJ_BR_ITEM")
//    엑셀에 맞는 데이터 추출
    val hjBrExcelData = getExcelData.getHjBrItemExcelData(hjBrItemRaw)
//    컬럼매핑 가져오기
    val hjBrExcelCol = jdbcGetObj.getItemColCD("HJ", "B")
//    엑셀에 맞는 컬럼으로 바꾸기
    val hjBrItem = getRenameObj.getHjItemExcelColNm(hjBrExcelData, hjBrExcelCol)

//    메뉴
//    메뉴 데이터 가져오기
    val hjMnRaw = jdbcGetObj.getCurrentMnIntr("HJ_MN_INTR")
//    필터 데이터 가져오기 (필터 설명 붙여야함)
    val hjFltrRaw = jdbcGetObj.getFltrRLTbl("HJ_FLTR_INTR")
//    엑셀에 맞는 데이터 추출
    val hjMnExcelData = getExcelData.getHjMnExcelData(hjMnRaw, hjFltrRaw)
//    컬럼 매핑 가져오기
    val hjMnExcelCol = jdbcGetObj.getMnColCD("HJ")
//    엑셀에 맞는 컬럼으로 바꾸기
    val hjMn = getRenameObj.getHjMnExcelColNm(hjMnExcelData, hjMnExcelCol)

    //    todo
    //    저장하기

    logger.info("[appName=communication] [function=runHjCommunication] [runStatus=start] [message=start]")
  }

  def runHj2Communication() = {
    logger.info("[appName=communication] [function=runHj2Communication] [runStatus=start] [message=start]")
    val jdbcGetObj = new jdbc.JdbcGet(spark)
    val getRenameObj = new GetHjExcelColNm(spark)

    val hj2MnRaw = jdbcGetObj.getHjMktCdTbl("HJ_MKT_CD")
    val hj2ItemRaw = jdbcGetObj.getHjAgrnItemTbl("HJ_AGRN_ITEM")

    val hj2SelectItem = new GetHjExcelData(spark).getHj2ItemData(hj2MnRaw, hj2ItemRaw)

    val hj2Item = getRenameObj.getHj2ItemExcelColNm(hj2SelectItem)
    val hj2Mn = getRenameObj.getHj2MnExcelColNm(hj2MnRaw)


    logger.info("[appName=communication] [function=runHj2Communication] [runStatus=start] [message=start]")
  }
}
