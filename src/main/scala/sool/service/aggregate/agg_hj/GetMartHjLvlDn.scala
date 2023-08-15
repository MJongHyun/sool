/**

 */
package sool.service.aggregate.agg_hj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace}

class GetMartHjLvlDn(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // HJ 마트의 주소 컬럼들을 레벨로 변경하고 일부 필요 컬럼 추가하기
  def addColOrRenameColNm(martHjBsDn: DataFrame) = {
    val martHjBsDnChng = martHjBsDn.
      withColumn("LEVEL0", regexp_replace($"ADDR0", "0전국", "전국")).
      withColumnRenamed("ADDR1", "LEVEL1").
      withColumnRenamed("ADDR2", "LEVEL2").
      withColumnRenamed("ADDR3D", "LEVEL3").
      withColumnRenamed("ADDR3R", "LEVEL4").
      withColumn("SLEVEL0", regexp_replace($"ADDR0_SUP", "0전국", "전국")).
      withColumnRenamed("ADDR1_SUP", "SLEVEL1").
      withColumnRenamed("ADDR2_SUP", "SLEVEL2").
      withColumnRenamed("ADDR3D_SUP", "SLEVEL3").
      withColumnRenamed("ADDR3R_SUP", "SLEVEL4").
      withColumnRenamed("POST_CD_SUP", "SPOST_CD")
    val martHjBsLvlDn = martHjBsDnChng.select(
      martHjBsDnChng.columns.filterNot(c => c.contains("ADDR")).map(col):_*
    )
    martHjBsLvlDn
  }

  // 메인
  def getMartHjLvlDn(martHjbDeno: DataFrame, martHjsDeno: DataFrame, martHjbNume: DataFrame, martHjsNume: DataFrame) = {
    val martHjbLvlDeno = addColOrRenameColNm(martHjbDeno)
    val martHjsLvlDeno = addColOrRenameColNm(martHjsDeno)
    val martHjbLvlNume = addColOrRenameColNm(martHjbNume)
    val martHjsLvlNume = addColOrRenameColNm(martHjsNume)
    (martHjbLvlDeno, martHjsLvlDeno, martHjbLvlNume, martHjsLvlNume)
  }
}
