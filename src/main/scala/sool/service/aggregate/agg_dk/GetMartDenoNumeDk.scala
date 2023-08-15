/**

 */
package sool.service.aggregate.agg_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class GetMartDenoNumeDk(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  // 맥주, 위스키별 분모, 분자 쿼리 테이블 생성
  def getDkMnFltrQryBw(dkFltrQry: DataFrame, dkMnIntr: DataFrame, option: String) = {
    // 옵션값(분모 or 분자)에 따라 데이터 조인하기 위한 컬럼명 설정
    val fltrIdColNm = if (option == "DENO") "FLTR_ID1" else if (option == "NUME") "FLTR_ID2" else ""

    val dkFltrQryReNmCol = dkFltrQry.withColumnRenamed("FLTR_ID", fltrIdColNm)
    val dkMnIntrSel = dkMnIntr.select('MN_ID, col(fltrIdColNm), 'MN_TYPE_CD, 'MN_NM) // 메뉴 개요에서 select
    val dkMnFltrQry = dkMnIntrSel.join(dkFltrQryReNmCol, Seq(fltrIdColNm)).
      select('MN_ID, 'QRY_C, 'MN_TYPE_CD).distinct // distinct 해줘야 한다.

    // 맥주, 위스키별 쿼리 테이블
    val dkMnFltrQryB = dkMnFltrQry.filter('MN_TYPE_CD === "B").drop("MN_TYPE_CD")
    val dkMnFltrQryW = dkMnFltrQry.filter('MN_TYPE_CD === "W").drop("MN_TYPE_CD")
    (dkMnFltrQryB, dkMnFltrQryW)
  }

  // 맥주, 위스키별 분모, 분자 데이터프레임 생성
  def getMartDkDenoNume(martDk: DataFrame, dkMnFltrQryDf: DataFrame) = {
    // 마트 데이터를 필터 ID별 쿼리문으로 필터링한다. 각 쿼리문으로 필터링 된 데이터 테이블들을 하나로 합친다.
    val martDkRes = dkMnFltrQryDf.as[(String, String)].collect.map(i => {
      val (mnId, qry) = (i._1, i._2)
      martDk.filter(qry).withColumn("MENU_ID", lit(mnId)) // DK 최종결과 메뉴아이디 컬럼명이 "MENU_ID"
    }).reduce(_ union _)
    martDkRes
  }

  // 해당 클래스의 메인 역할을 하는 함수
  def getMartDenoNumeDK(martDkb: DataFrame,
                        martDkw: DataFrame,
                        dkMnIntr: DataFrame,
                        dkFltrQry: DataFrame) = {
    // 맥주, 위스키별 분모 쿼리 테이블
    val (dkMnFltrQryDenoB, dkMnFltrQryDenoW) = getDkMnFltrQryBw(dkFltrQry, dkMnIntr, "DENO")

    // 맥주, 위스키별 분자 쿼리 테이블
    val (dkMnFltrQryNumeB, dkMnFltrQryNumeW) = getDkMnFltrQryBw(dkFltrQry, dkMnIntr, "NUME")

    // 맥주, 소주별 분모, 분자 데이터프레임 생성
    val martDkbDeno = getMartDkDenoNume(martDkb, dkMnFltrQryDenoB)
    val martDkwDeno = getMartDkDenoNume(martDkw, dkMnFltrQryDenoW)
    val martDkbNume = getMartDkDenoNume(martDkb, dkMnFltrQryNumeB)
    val martDkwNume = getMartDkDenoNume(martDkw, dkMnFltrQryNumeW)

    (martDkbDeno, martDkwDeno, martDkbNume, martDkwNume)
  }
}
