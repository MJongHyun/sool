/**
 * HJ 집계 결과 생성

 */
package sool.service.aggregate.agg_hj

import sool.service.run.RunService.logger
import sool.service.aggregate.agg_hj._

class RunAggHj(spark: org.apache.spark.sql.SparkSession) {
  // 집계에 필요한 클래스들 선언
  val getAggDfsHjCls = new GetAggDfsHj(spark) // 기초 데이터 관련 클래스
  val getDenoNumeDfHjCls = new GetMartDenoNumeHj(spark) // 분모, 분자 데이터프레임 생성 관련 클래스
  val getMartHjLvlDnCls = new GetMartHjLvlDn(spark) // 주소 관련 컬럼명 수정 및 컬럼 추가
  val getAnvrDfsHjCls = new GetAnvrDfsHj(spark) // 주소 관련 레벨별 데이터프레임 생성
  val getAnvr13DfsHjCls = new GetAnvr13DfsHj(spark) // ANVR13 생성 관련 클래스
  val saveDfsHjCls = new SaveDfsHj(spark) // 저장 관련 클래스

  // 메인
  def runAggHj(ethDt:String, flag: String) = {
    logger.info("[appName=sool] [function=runAggHj] [runStatus=start] [message=start]")

    // HJ 집계에 필요한 데이터 로드
    val (hjbItem, hjsItem) = getAggDfsHjCls.getHjItemTbl()
    val (martHjb, martHjs, martHjbNonAddr, martHjsNonAddr) = getAggDfsHjCls.getMartHj(ethDt, flag)  // 맥주, 소주 마트
    val (hjMnIntr, hjFltrIntr, hjFltrRl) = getAggDfsHjCls.getHjMnFltrDfs(ethDt)  // 메뉴 개요, 필터 개요, 필터 규칙 테이블 로드
    val (comMain, lv2w, supCdDimension, cdAnvrParquet) = getAggDfsHjCls.getDfsHj(ethDt, flag)
    val hjFltrQry = getAggDfsHjCls.getHjFltrQry(hjFltrRl, hjFltrIntr)

    // 맥주, 소주별 분모, 분자 데이터프레임 생성
    val (martHjbDeno, martHjsDeno, martHjbNume, martHjsNume) = getDenoNumeDfHjCls.
      getMartDenoNumeHj(martHjb, martHjs, hjMnIntr, hjFltrQry)

    // 맥주, 소주별 분모, 분자 데이터프레임에 주소 관련 컬럼명 수정 및 컬럼 추가(Lvl)
    val (martHjbLvlDeno, martHjsLvlDeno, martHjbLvlNume, martHjsLvlNume) = getMartHjLvlDnCls.
      getMartHjLvlDn(martHjbDeno, martHjsDeno, martHjbNume, martHjsNume)

    // ANVR01 ~ ANVR06 생성
    val (anvr01, anvr02, anvr03, anvr04, anvr05, anvr06) = getAnvrDfsHjCls.
      getAnvrDfsHj(martHjbLvlDeno, martHjsLvlDeno, martHjbLvlNume, martHjsLvlNume, hjMnIntr, ethDt)


    // ANVR01 ~ ANVR06 을 parquet 으로 저장
    saveDfsHjCls.saveAnvrDfs(ethDt, anvr01, anvr02, anvr03, anvr04, anvr05, anvr06, flag)
    // CD_DIMENSION 저장
    saveDfsHjCls.saveCdDimension(ethDt, hjMnIntr, hjFltrIntr, flag)
    // CD_ANVR 저장
    saveDfsHjCls.saveCdAnvr(ethDt, cdAnvrParquet, flag)
    // ANVR01 ~ ANVR06, ADDR01 ~ ADDR06 을 TSV 로 저장
    saveDfsHjCls.saveAnvrAddrTsv(ethDt, flag)
    // lv2w 저장
    saveDfsHjCls.writeCdTbls(ethDt, lv2w, "LV2W_MAP", flag)

    // ANVR13 관련 결과물
    // 1) 공급업체 기준 맥주, 소주 마트, 2) 공급업체 기준 분모, 분자 마트, 3) ANVR13 결과
    val (hjbSupMart, hjsSupMart, hjSupDenoNume, anvr13) = getAnvr13DfsHjCls.
      getAnvr13DfsHj(ethDt, comMain, lv2w, martHjbNonAddr, martHjsNonAddr, hjbItem, hjsItem, supCdDimension, hjFltrQry)

    // 공급업체 기준 맥주, 소주 마트 저장
    saveDfsHjCls.saveHjSupMart(ethDt, hjbSupMart, hjsSupMart, flag)
    // 공급업체 기준 맥주 + 소주 통합 분모, 분자 마트 저장
    saveDfsHjCls.saveHjSupDenoNume(ethDt, hjSupDenoNume, flag)
    // ANVR13 저장
    saveDfsHjCls.saveAnvr13Tsv(ethDt, anvr13, flag)

    logger.info("[appName=sool] [function=runAggHj] [runStatus=end] [message=end]")
  }
}