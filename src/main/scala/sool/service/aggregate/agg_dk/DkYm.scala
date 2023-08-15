/**

 */
package sool.service.aggregate.agg_dk

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class DkYm() extends java.io.Serializable {
  // 현재 집계연월을 DK 회계연월로 바꾸기
  def apply(ethDt: String) = {
    val dtFrmt1 = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dtFrmt2 = DateTimeFormatter.ofPattern("yy-MM")
    val dt = LocalDate.parse(ethDt + "01", dtFrmt1)
    val dkDt = dt.plusMonths(6).format(dtFrmt2)
    val dkDtList = dkDt.split("-")
    val dkYm = s"F${dkDtList(0)}P${dkDtList(1)}"  // 연월
    val dkY = s"F${dkDtList(0)}"  // 연
    val dkM = s"P${dkDtList(1)}"  // 월
    (dkYm, dkY, dkM)
  }
}
