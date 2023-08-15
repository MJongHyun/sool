/**

 */
package sool.service.aggregate.agg_hj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class GetMartDenoNumeHj(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 맥주, 소주별 쿼리 테이블 생성
  def getHjMnFltrQryBS(hjFltrQry:DataFrame, hjMnIntr:DataFrame, option:String) = {
    // 옵션값(분모 or 분자)에 따라 데이터 조인하기 위한 컬럼명 설정
    val fltrIdColNm = if (option == "DENO") "FLTR_ID1" else if (option == "NUME") "FLTR_ID2" else ""
    // 옵션값(분모 or 분자)에 따라 HJ 결과 컬럼명에 맞는 컬럼명 설정
    val fltrIdColReNm = if (option == "DENO") "DENOM_CD" else if (option == "NUME") "NUMER_CD" else ""

    val hjFltrQryReNmCol = hjFltrQry.withColumnRenamed("FLTR_ID", fltrIdColNm)
    val hjMnIntrSel = hjMnIntr.select('MN_ID, col(fltrIdColNm), 'MN_TYPE_CD, 'MN_NM)  // 메뉴 개요에서 select
    val hjMnFltrQry = hjMnIntrSel.join(hjFltrQryReNmCol, Seq(fltrIdColNm)).
      select(col(fltrIdColNm).as(fltrIdColReNm), 'QRY_C, 'MN_TYPE_CD).distinct  // distinct 해줘야 한다.

    // 맥주, 소주별 쿼리 테이블
    val hjMnFltrQryB = hjMnFltrQry.filter('MN_TYPE_CD === "B").drop("MN_TYPE_CD")
    val hjMnFltrQryS = hjMnFltrQry.filter('MN_TYPE_CD === "S").drop("MN_TYPE_CD")
    (hjMnFltrQryB, hjMnFltrQryS)
  }

  // 맥주, 소주별 분모, 분자 데이터프레임 생성
  def getMartHjDenoNume(martHj:DataFrame, hjMnFltrQryDf:DataFrame, option:String) = {
    val cdColNm = if (option == "DENO") "DENOM_CD" else if (option == "NUME") "NUMER_CD" else ""

    // 마트 데이터를 필터 ID별 쿼리문으로 필터링한다. 각 쿼리문으로 필터링 된 데이터 테이블들을 하나로 합친다.
    val martHjRes = hjMnFltrQryDf.as[(String, String)].collect.map(i => {
      val (cd, qry) = (i._1, i._2)
      martHj.filter(qry).withColumn(cdColNm, lit(cd))
    }).reduce(_ union _)
    martHjRes
  }

  // 해당 클래스의 메인 역할을 하는 함수
  def getMartDenoNumeHj(martHjb: DataFrame, martHjs: DataFrame, hjMnIntr: DataFrame, hjFltrQry: DataFrame) = {
    // 맥주, 소주별 분모 쿼리 테이블
    val (hjMnFltrQryDenoB, hjMnFltrQryDenoS) = getHjMnFltrQryBS(hjFltrQry, hjMnIntr, "DENO")

    // 맥주, 소주별 분자 쿼리 테이블
    val (hjMnFltrQryNumeB, hjMnFltrQryNumeS) = getHjMnFltrQryBS(hjFltrQry, hjMnIntr, "NUME")

    // 맥주, 소주별 분모, 분자 데이터프레임 생성
    val martHjbDeno = getMartHjDenoNume(martHjb, hjMnFltrQryDenoB, "DENO")
    val martHjsDeno = getMartHjDenoNume(martHjs, hjMnFltrQryDenoS, "DENO")
    val martHjbNume = getMartHjDenoNume(martHjb, hjMnFltrQryNumeB, "NUME")
    val martHjsNume = getMartHjDenoNume(martHjs, hjMnFltrQryNumeS, "NUME")

    (martHjbDeno, martHjsDeno, martHjbNume, martHjsNume)
  }
}
