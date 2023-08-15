package sool.communication.get_communication_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class GetDkExcelColNm (spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  //  아이템 마스터 커뮤니케이션 엑셀 기준 컬럼으로 가져오기
  def getDkItemExcelColNm(itemRaw: DataFrame, excelCol: DataFrame) = {
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

  // DK-  메뉴 커뮤니케이션 엑셀 기준 컬럼으로 가져오기
  def getDkMnExcelColNm(dkMnExcelData: DataFrame, dkMnExcelCol: DataFrame) = {
    //    엑셀 컬럼 Map
    val colNmMap = dkMnExcelCol.
      filter('NW_COL_NM =!= "FLTR_DESC_TXT").
      select('NW_COL_NM, 'PRE_COL_NM).
      distinct.
      as[(String, String)].
      collect.
      toMap

    val itemColMap = dkMnExcelData.columns.map(c => col(c).as(colNmMap.getOrElse(c, c)))
    val reColNmItem = dkMnExcelData.select(itemColMap: _*)

    val itemExcelCol = reColNmItem.orderBy('MENU_ID)

    itemExcelCol
  }
}