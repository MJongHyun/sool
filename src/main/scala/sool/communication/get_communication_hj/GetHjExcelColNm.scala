package sool.communication.get_communication_hj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class GetHjExcelColNm (spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  //  아이템 마스터 커뮤니케이션 엑셀 기준 컬럼으로 가져오기
  def getHjItemExcelColNm(itemRaw: DataFrame, excelCol: DataFrame) = {
    val colSeqList = excelCol.
      orderBy('CD_ID).
      select('PRE_COL_NM).
      as[String].
      collect()

    val colList = "NO" +: colSeqList

    //    엑셀 컬럼 Map
    val colNmMap = excelCol.
      select('NW_COL_NM, 'PRE_COL_NM).
      as[(String, String)].
      collect.
      toMap

    //    아이템 마스터
    val itemColMap = itemRaw.columns.map(c => col(c).as(colNmMap.getOrElse(c, c)))
    val reColNmItem = itemRaw.select(itemColMap: _*)

    val itemExcelCol = reColNmItem.select(colList.map(col):_*)

    itemExcelCol
  }

  //  메뉴 커뮤니케이션 엑셀 기준 컬럼으로 가져오기
  def getHjMnExcelColNm(hjMnExcelData: DataFrame, hjMnExcelCol: DataFrame) = {
    //    엑셀 컬럼 Map
    val colNmMap = hjMnExcelCol.
      select('NW_COL_NM, 'PRE_COL_NM).
      as[(String, String)].
      collect.
      toMap

    val itemColMap = hjMnExcelData.columns.map(c => col(c).as(colNmMap.getOrElse(c, c)))
    val reColNmItem = hjMnExcelData.select(itemColMap: _*)

    val itemExcelCol = reColNmItem.orderBy('MENU_IDX)

    itemExcelCol
  }

//  HJ 2차 아이템 엑셀 기준 컬럼으로 가져오기
  def getHj2ItemExcelColNm(hj2SelectItem: DataFrame) = {
    val hj2Item = hj2SelectItem.select('ITEM_NM as "ITEM",
      'MKT_NM as "MARKET_NAME")

    hj2Item
  }

//  HJ 2차 메뉴 엑셀 기준 컬럼으로 가져오기
  def getHj2MnExcelColNm(hj2MnRaw: DataFrame) = {
    val hj2Mn = hj2MnRaw.select('MKT_NM as "MARKET_NAME",
      'MKT_CD as "MARKET_CODE")

    hj2Mn
  }
}