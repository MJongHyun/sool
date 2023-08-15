/**
 * AG 결과 저장

 */
package sool.service.aggregate.agg_anglnt

import org.apache.spark.sql.DataFrame
import sool.common.jdbc.JdbcSave

class SaveAnglNtRslt(spark: org.apache.spark.sql.SparkSession) {
  // 필요 클래스 선언
  val jdbcSaveCls = new JdbcSave()

  // AG 결과 데이터 DB에 업로드
  def saveAnglNtRslt(anglNtRslt: DataFrame) = {
    jdbcSaveCls.saveAnglNtRsltTbl(anglNtRslt)
  }
}