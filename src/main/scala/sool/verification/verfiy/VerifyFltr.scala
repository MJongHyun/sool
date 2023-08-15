package sool.verification.verfiy

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{collect_list, concat, concat_ws, lit, when}
import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import sool.service.get_marts.get_mart_hj.SaveMartHj
import sool.service.aggregate.agg_hj.GetMartDenoNumeHj


class VerifyFltr(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)
  val saveMartHjCls = new SaveMartHj(spark)
  val fileFuncCls = new FileFunc(spark)
  val hjFltrCls = new GetMartDenoNumeHj(spark)


  def getHjFltrQry(hjFltrRl:DataFrame, hjFltrIntr:DataFrame) ={
    // CND_VAL 에 따옴표 붙이기, DQ : Double Quote
    val hjFltrRlAddDqCol = hjFltrRl.
      withColumn("CND_VAL_SQ", concat(lit("\""), 'CND_VAL, lit("\"")))

    // CND_VAL 하나로 합치기
    val hjFltrRlClctVal = hjFltrRlAddDqCol.groupBy('FLTR_ID, 'ITEM_COL_CD, 'CND_OPR).
      agg(concat_ws(", ", collect_list("CND_VAL_SQ")).as("CND_VAL_C"))

    // 개별 쿼리문 생성
    val hjFltrRlClctValOne = hjFltrRlAddDqCol.groupBy('FLTR_ID, 'ITEM_COL_CD, 'CND_OPR, 'CND_VAL).
      agg(concat_ws(", ", collect_list("CND_VAL_SQ")).as("CND_VAL_C"))

    // "in" 혹은 "not in" 에 해당하는 CND_VAL 은 괄호로 묶어주기
    val hjFltrRlGModValC = hjFltrRlClctVal.withColumn("CND_VAL_C",
      when('CND_OPR === "in" or 'CND_OPR === "not in",
        concat(lit("("), 'CND_VAL_C, lit(")"))).otherwise('CND_VAL_C))

    // "in" 혹은 "not in" 에 해당하는 CND_VAL 은 괄호로 묶어주기
    val hjFltrRlGModValCOne = hjFltrRlClctValOne.withColumn("CND_VAL_C",
      when('CND_OPR === "in" or 'CND_OPR === "not in",
        concat(lit("("), 'CND_VAL_C, lit(")"))).otherwise('CND_VAL_C))

    // 각각 개별 쿼리문 생성
    val hjFltrRlAddQryCol = hjFltrRlGModValC.withColumn("QRY",
      concat('ITEM_COL_CD, lit(" "), 'CND_OPR, lit(" "), 'CND_VAL_C))

    // 각각 개별 쿼리문 생성
    val hjFltrRlAddQryColOne = hjFltrRlGModValCOne.withColumn("QRY",
      concat('ITEM_COL_CD, lit(" "), 'CND_OPR, lit(" "), 'CND_VAL_C))

    // 필터 아이디별 쿼리문들 묶어주기
    val hjFltrRlQry = hjFltrRlAddQryCol.groupBy('FLTR_ID).
      agg(concat_ws(" AND ", collect_list("QRY")).as("QRY_C"))

    // 필터 개요 테이블이랑 조인하기
    val hjFltrQry = hjFltrIntr.join(hjFltrRlQry, Seq("FLTR_ID"))
    //개별 쿼리문 조인
    val hjFltrQryOne = hjFltrIntr.join(hjFltrRlAddQryColOne, Seq("FLTR_ID")).withColumnRenamed("QRY","QRY_C")
    (hjFltrQry, hjFltrQryOne)
  }

  // HJ 메뉴 및 필터 테이블 로드
  def getHjMnFltrDfs(ethDt: String) = {
    val hjBrItem = jdbcGetCls.getItemTbl("HJ_BR_ITEM")
    val hjSjItem = jdbcGetCls.getItemTbl("HJ_SJ_ITEM")

    val hjMnIntr = jdbcGetCls.getMnIntrTbl("HJ_MN_INTR", ethDt)
    val hjFltrIntr = jdbcGetCls.getFltrIntrTbl("HJ_FLTR_INTR")
    val hjFltrRl = jdbcGetCls.getFltrRLTbl("HJ_FLTR_RL")
    (hjBrItem, hjSjItem, hjMnIntr, hjFltrIntr, hjFltrRl)
  }

  def getMartHjDenoNumeOne(martHj:DataFrame, hjMnFltrQryDf:DataFrame, option:String) = {
    val cdColNm = if (option == "DENO") "DENOM_CD" else if (option == "NUME") "NUMER_CD" else ""

    // 마트 데이터를 필터 ID별 쿼리문으로 필터링한다. 각 쿼리문으로 필터링 된 데이터 테이블들을 하나로 합친다.
    val martHjRes = hjMnFltrQryDf.as[(String, String)].collect.map(i => {
      val (cd, qry) = (i._1, i._2)
      martHj.filter(qry).withColumn(cdColNm, lit(cd)).withColumn("rownum", functions.monotonically_increasing_id()).filter($"rownum"===0)
    }).reduce(_ union _)
    martHjRes
  }

  def runVerifyFltr(ethDt: String, flag: String): Unit = {
    // 데이터 로드
    val (hjBrItem, hjSjItem, hjMnIntr, hjFltrIntr, hjFltrRl) = getHjMnFltrDfs(ethDt: String)
    // 쿼리 생
    val (hjFltrQry, hjFltrQryOne) = getHjFltrQry(hjFltrRl, hjFltrIntr)
    // 맥주, 소주별 분모 쿼리 테이블
    val (hjMnFltrQryDenoB, hjMnFltrQryDenoS) = hjFltrCls.getHjMnFltrQryBS(hjFltrQry, hjMnIntr, "DENO")
    // 맥주, 소주별 분자 쿼리 테이블
    val (hjMnFltrQryNumeB, hjMnFltrQryNumeS) = hjFltrCls.getHjMnFltrQryBS(hjFltrQry, hjMnIntr, "NUME")
    // 맥주, 소주별 분모, 분자 데이터프레임 생성
    val martHjbDeno = hjFltrCls.getMartHjDenoNume(hjBrItem, hjMnFltrQryDenoB, "DENO")
    val martHjbNume = hjFltrCls.getMartHjDenoNume(hjBrItem, hjMnFltrQryNumeB, "NUME")
    val martHjsDeno = hjFltrCls.getMartHjDenoNume(hjSjItem, hjMnFltrQryDenoS, "DENO")
    val martHjsNume = hjFltrCls.getMartHjDenoNume(hjSjItem, hjMnFltrQryNumeS, "NUME")

    // 개별 쿼리
    // 맥주, 소주별 분모 쿼리 테이블
    val (hjMnFltrQryDenoBOne, hjMnFltrQryDenoSOne) = hjFltrCls.getHjMnFltrQryBS(hjFltrQryOne, hjMnIntr, "DENO")
    // 맥주, 소주별 분자 쿼리 테이블
    val (hjMnFltrQryNumeBOne, hjMnFltrQryNumeSOne) = hjFltrCls.getHjMnFltrQryBS(hjFltrQryOne, hjMnIntr, "NUME")
    val hjMnFltrQryDenoB_One_t = hjMnFltrQryDenoBOne.withColumn("rownum", functions.monotonically_increasing_id()).withColumn("DENOM_CD_2", concat($"DENOM_CD", lit("_"), $"rownum")).drop("rownum","DENOM_CD").withColumnRenamed("DENOM_CD_2","DENOM_CD").select("DENOM_CD","QRY_C")
    val hjMnFltrQryDenoS_One_t = hjMnFltrQryDenoSOne.withColumn("rownum", functions.monotonically_increasing_id()).withColumn("DENOM_CD_2", concat($"DENOM_CD", lit("_"), $"rownum")).drop("rownum","DENOM_CD").withColumnRenamed("DENOM_CD_2","DENOM_CD").select("DENOM_CD","QRY_C")
    val hjMnFltrQryNumeB_One_t = hjMnFltrQryNumeBOne.withColumn("rownum", functions.monotonically_increasing_id()).withColumn("NUMER_CD_2", concat($"NUMER_CD", lit("_"), $"rownum")).drop("rownum","NUMER_CD").withColumnRenamed("NUMER_CD_2","NUMER_CD").select("NUMER_CD","QRY_C")
    val hjMnFltrQryNumeS_One_t = hjMnFltrQryNumeSOne.withColumn("rownum", functions.monotonically_increasing_id()).withColumn("NUMER_CD_2", concat($"NUMER_CD", lit("_"), $"rownum")).drop("rownum","NUMER_CD").withColumnRenamed("NUMER_CD_2","NUMER_CD").select("NUMER_CD","QRY_C")
    // 맥주, 소주별 분모, 분자 데이터프레임 생성
    val martHjbDenoOne = getMartHjDenoNumeOne(hjBrItem, hjMnFltrQryDenoB_One_t, "DENO")
    val martHjbNumeOne = getMartHjDenoNumeOne(hjBrItem, hjMnFltrQryNumeB_One_t, "NUME")
    val martHjsDenoOne = getMartHjDenoNumeOne(hjSjItem, hjMnFltrQryDenoS_One_t, "DENO")
    val martHjsNumeOne = getMartHjDenoNumeOne(hjSjItem, hjMnFltrQryNumeS_One_t, "NUME")

    // 아이템 누락 확인
    val hjAllItem = hjBrItem.select("BR_NM").union(hjSjItem.select("SJ_NM"))
    val filterItem = martHjbDeno.select("BR_NM").distinct.union(martHjbNume.select("BR_NM").distinct).union(martHjsDeno.select("SJ_NM")).union(martHjsNume.select("SJ_NM").distinct).distinct

    if (filterItem.except(hjAllItem).count()>0 || hjAllItem.except(filterItem).count()>0){
      println("아이템 누락 확인 필요")
      filterItem.except(hjAllItem).show(100, false)
      hjAllItem.except(filterItem).show(10, false)
    }else{
      println("아이템 누락 없음")
    }
    // 메뉴별 쿼리 - 메뉴 코드가 누락시 출력
    if (martHjbDeno.select("DENOM_CD").distinct.count != hjMnFltrQryDenoB.select("DENOM_CD").distinct.count){
      println("메뉴 코드 누락 확인 필요")
      hjMnFltrQryDenoB.select("DENOM_CD").distinct.except(martHjbDeno.select("DENOM_CD").distinct).show(100, false)
    }else{
      println("맥주 DENOM_CD 누락 없음")
    }
    if (martHjbNume.select("NUMER_CD").distinct.count != hjMnFltrQryNumeB.select("NUMER_CD").distinct.count)
    {
      println("메뉴 코드 누락 확인 필요")
      hjMnFltrQryNumeB.select("DENOM_CD").distinct.except(martHjbNume.select("DENOM_CD").distinct).show(100, false)
    } else{
      println("맥주 NUMER_CD 누락 없음")
    }
    if (martHjsDeno.select("DENOM_CD").distinct.count != hjMnFltrQryDenoS.select("DENOM_CD").distinct.count)
    {
      println("메뉴 코드 누락 확인 필요")
      hjMnFltrQryDenoS.select("DENOM_CD").distinct.except(martHjsDeno.select("DENOM_CD").distinct).show(100, false)
    } else{
      println("소주 DENOM_CD 누락 없음")
    }
    if (martHjsNume.select("NUMER_CD").distinct.count != hjMnFltrQryNumeS.select("NUMER_CD").distinct.count)
    {
      println("메뉴 코드 누락 확인 필요")
      hjMnFltrQryNumeS.select("DENOM_CD").distinct.except(martHjsNume.select("DENOM_CD").distinct).show(100, false)
    }else{
      println("소주 NUMER_CD 누락 없음")
    }
    // 개별 쿼리 - 메뉴 코드가 누락시 출력
    if (martHjbDenoOne.select("DENOM_CD").distinct.count != (hjMnFltrQryDenoB_One_t.select("DENOM_CD").distinct.count)) {
      println("메뉴 코드 누락 확인 필요")
      hjMnFltrQryDenoB_One_t.select("DENOM_CD").distinct.except(martHjbDenoOne.select("DENOM_CD").distinct).show(100, false)
    }else{
      println("맥주 개별 쿼리 DENOM_CD 누락 없음")
    }
    if (martHjbNumeOne.select("NUMER_CD").distinct.count != hjMnFltrQryNumeB_One_t.select("NUMER_CD").distinct.count){
      println("메뉴 코드 누락 확인 필요")
      hjMnFltrQryNumeB_One_t.select("NUMER_CD").distinct.except(martHjbNumeOne.select("NUMER_CD").distinct).show(100, false)
    }else{
      println("맥주 개별 쿼리 NUMER_CD 누락 없음")
    }
    if (martHjsDenoOne.select("DENOM_CD").distinct.count != hjMnFltrQryDenoS_One_t.select("DENOM_CD").distinct.count){
      println("메뉴 코드 누락 확인 필요")
      hjMnFltrQryDenoS_One_t.select("DENOM_CD").distinct.except(martHjsDenoOne.select("DENOM_CD").distinct).show(100, false)
    }else{
      println("소주 개별 쿼리 DENOM_CD 누락 없음")
    }
    if (martHjsNumeOne.select("NUMER_CD").distinct.count != hjMnFltrQryNumeS_One_t.select("NUMER_CD").distinct.count){
      println("메뉴 코드 누락 확인 필요")
      hjMnFltrQryNumeS_One_t.select("DENOM_CD").distinct.except(martHjsNumeOne.select("DENOM_CD").distinct).show(100, false)
    }else{
      println("소주 개별 쿼리 NUMER_CD 누락 없음")
    }
  }
}
