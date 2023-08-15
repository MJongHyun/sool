/**
 * save 기능 jdbc func

 */
package sool.common.jdbc

import org.apache.spark.sql.{DataFrame, SaveMode}
import sool.common.function.GetTime

class JdbcSave () {
  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val jdbcPropCls = new JdbcProp()

  // AG 결과 저장
  def saveAnglNtRsltTbl(anglNtRslt: DataFrame) = {
    val (url, connProp) = jdbcPropCls.jdbcPropDkTest()
    anglNtRslt.write.mode(SaveMode.Append).jdbc(url, "DK_WHISKY_RETAILER", connProp)
    println(s"${getTimeCls.getTimeStamp()}, DK_WHISKY_RETAILER 테이블에 write 되었습니다.")
  }

  // upsert DTI_ADDRESS
  def upsertDtiAdrs(rfnAddrForDb: DataFrame) = {
    val (conn, stmt) = jdbcPropCls.soolAdrsJdbcStmt()

    // DB Upsert 명령 (qry 문 변경하지 말것)
    rfnAddrForDb.collect.foreach(row => {
      val (bldngMgmtNmb, pnu, srcAdrs, srchRsltsCnt) = (row(0), row(1), row(2), row(3))
      val tblNm = "DTI_ADDRESS"
      val qry = s"""INSERT INTO `${tblNm}` (`BLDNG_MGMT_NMB`, `PNU`, `SRC_ADRS`, `SRCH_RSLTS_CNT`) """ +
        s"""VALUES (${bldngMgmtNmb}, ${pnu}, "${srcAdrs}", ${srchRsltsCnt}) """ +
        s"""ON DUPLICATE KEY UPDATE BLDNG_MGMT_NMB = """ +
        s"""${bldngMgmtNmb}, PNU = ${pnu}, SRC_ADRS = "${srcAdrs}", SRCH_RSLTS_CNT = ${srchRsltsCnt}"""
      try {
        stmt.execute(qry)   // DB execute
      } catch {
        case ex: Exception => {
          println("*** 에러가 발생했습니다. ***")
          println(qry)
          println(ex)
          println("*** 에러가 발생했습니다. ***")
        }
      }
    })
    jdbcPropCls.closeDbJdbc(conn, stmt)
  }
}
