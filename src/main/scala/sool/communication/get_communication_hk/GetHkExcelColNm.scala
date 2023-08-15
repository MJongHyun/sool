package sool.communication.get_communication_hk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class GetHkExcelColNm  (spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  def getHkExcelColNm(hkItemRaw: DataFrame, hkExcelCol: DataFrame) = {
    val colNmMap = hkExcelCol.
      select('NW_COL_NM, 'PRE_COL_NM).
      as[(String, String)].
      collect.
      toMap

    val itemColMap = hkItemRaw.columns.map(c => col(c).as(colNmMap.getOrElse(c, c)))
    val reColNmItem = hkItemRaw.select(itemColMap: _*)

    val itemExcelCol = reColNmItem.orderBy('ITEM_ID)
    val hkItem = itemExcelCol.drop('ITEM_ID)

    hkItem
  }
}
