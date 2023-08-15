/**
 * 주류 데이터 추출

 */
package sool.extract.extract_data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, udf}
import sool.common.function.FileFunc
import sool.common.path.FilePath

class RunExtrctDtiEthanol(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  // 비주류 업체 리스트 전처리
  def getRemoveDf(nonEthCom: DataFrame) = {
    val nonEthComSlct = nonEthCom.select('COM_RGNO.as("SUP_RGNO"))
    val removeDf = nonEthComSlct.withColumn("ETHANOLYN", lit("N"))
    removeDf
  }

  // dti 필터링
  def getDtiFltr(dti: DataFrame) = {
    val dtiFltr = dti.
      filter('ITEM_NM.isNotNull).
      filter('ITEM_SZ.isNotNull).
      filter('ITEM_UP.isNotNull).
      filter('ITEM_QT.isNotNull).
      filter('ITEM_QT =!= 0.0).
      filter('SUP_AMT =!= 0.0)
//      filter('SUP_RGNO =!= "")  // 기존 주석 백업
    dtiFltr
  }

  // dti 에서 비주류 업체 제거
  def getDtiFltrExceptRemoveDf(dtiFltr: DataFrame, removeDf: DataFrame) = {
    val dtiFltrWithRemoveDf = dtiFltr.join(removeDf, Seq("SUP_RGNO"), "left_outer")
    val dtiFltrExceptRemoveDf = dtiFltrWithRemoveDf.
      filter('ETHANOLYN.isNull).
      drop('ETHANOLYN)
    dtiFltrExceptRemoveDf
  }

  def getItemUdf() = {
    /*
    기존 Zeppelin 주석
    수정사항: 바코드 10개 이상으로 수정 - 추후 13개 이하 짜리가 적힐 case가 충분히 있을것으로 생각되고,
    일반적으로 추출로직의 경우 가능한 case는 열어두는 것이 좋을 것으로 생각됩니다.
    */
    val itemUdf = udf((item: String)=> {
      if (("(\\d{0,2}[:;]){2,}".r.findAllIn(item).size > 0) || ("\\d{10,};".r.findAllIn(item).size > 0)) "Y" else "N"
    })
    itemUdf
  }

  // 주류 데이터 추출
  def extrctDtiEthanol(dtiFltrExceptRemoveDf: DataFrame) = {
    val itemUdf = getItemUdf()
    val dtiEthanol = dtiFltrExceptRemoveDf.
      withColumn("ETHANOL", itemUdf('ITEM_NM)).
      filter('ETHANOL === "Y").
      drop("ETHANOL")
    dtiEthanol
  }

  // 메인
  def runExtrctDtiEthanol(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)

    // 비주류 업체 리스트
    val nonEthCom = spark.read.parquet(filePathCls.baseCmmnNonEthComPath)
    val removeDf = getRemoveDf(nonEthCom)

    // dti
    val dti = spark.read.parquet(filePathCls.dtiPath)
    val dtiFltr = getDtiFltr(dti)

    // dti 에서 비주류 업체 제거
    val dtiFltrExceptRemoveDf = getDtiFltrExceptRemoveDf(dtiFltr, removeDf)

    // 주류 데이터 추출
    val dtiEthanol = extrctDtiEthanol(dtiFltrExceptRemoveDf)
    fileFuncCls.wParquet(dtiEthanol, filePathCls.dtiEthPath)  // 저장
  }
}
