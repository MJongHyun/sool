/**
 * DK 집계 결과 생성

 */
package sool.service.aggregate.agg_dk

import sool.common.function.FileFunc
import sool.service.run.RunService.logger
import sool.service.aggregate.agg_dk._

class RunAggDk(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  // 집계에 필요한 클래스 선언
  val getAggDfsDkCls = new GetAggDfsDk(spark)
  val getMartDenoNumeDkCls = new GetMartDenoNumeDk(spark)
  val getGroupByAddrDfsDkCls = new GetGroupByAddrDfsDk(spark)
  val getMenuAnlysResDfDkCls = new GetMenuAnlysResDfDk(spark)
  val getTotalDkCls = new GetTotalDk(spark)
  val getViewResCls = new GetViewRes(spark)
  val saveDfsDkCls = new SaveDfsDk(spark)
  val fileFuncCls = new FileFunc(spark)
  val resToDkTestDbCls = new ResToDkTestDb(spark)

  // 메인
  def runAggDk(ethDt: String, flag: String) = {
    logger.info(s"[file=RunAggDk] [function=runAggDk] [status=start] [message=DK ${ethDt} 데이터 집계 시작]")

    // 기초 데이터 로드
    val (dkMnIntr, dkFltrIntr, dkFltrRl) = getAggDfsDkCls.getDkMnFltrDfs(ethDt)
    val (martDkb, martDkw) = getAggDfsDkCls.getMartDk(ethDt, flag)
    val dkFltrQry = getAggDfsDkCls.getDkFltrQry(dkFltrRl, dkFltrIntr)
    val menuAnlysResDfBf1m = getAggDfsDkCls.getMenuAnlysResDfBf1m(ethDt, flag)
    val (hstryDkbBf1m, hstryDkwBf1m) = getAggDfsDkCls.getHstryDkbwBf1m(ethDt, flag)


    // 분모, 분자 마트 생성
    val (martDkbDeno, martDkwDeno, martDkbNume, martDkwNume) = getMartDenoNumeDkCls.getMartDenoNumeDK(
      martDkb, martDkw, dkMnIntr, dkFltrQry
    )

    // 분모, 분자 MENU_ID 및 주소별 그룹화
    val (martDkDenoAddrG, martDkNumeAddrG) = getGroupByAddrDfsDkCls.getGroupByAddrDfsDk(
      martDkbDeno, martDkwDeno, martDkbNume, martDkwNume
    )

    // 이번 달 분석 결과 생성
    val menuAnlysResDf = getMenuAnlysResDfDkCls.getMenuAnlysResDfDk(
      ethDt, martDkDenoAddrG, martDkNumeAddrG, menuAnlysResDfBf1m
    )

     saveDfsDkCls.saveMenuAnlysResDf(ethDt, menuAnlysResDf, flag)  // 이번 달 분석 결과 저장
     saveDfsDkCls.saveHstryDkbw(ethDt, menuAnlysResDfBf1m, hstryDkbBf1m, hstryDkwBf1m, flag) // 이번 달 히스토리 저장

    // 이번 달 분석 결과 및 이번 달 히스토리에서 필요한 데이터만 추출
    val (resDkb, resDkw) = getAggDfsDkCls.getMenuAnlysResDf(ethDt, flag)
    val (hstryDkb, hstryDkw) = getAggDfsDkCls.getHstryDkbw(ethDt, flag)

    /*
    1) EU 단위 변환, 백만 단위 변환, 반올림, 마스킹, DK 회계 연월 변환,
    2) 해당 집계 연월 데이터 + 과거 히스토리 데이터
    */
    val (totalDkb, totalDkw) = getTotalDkCls.getTotalDk(ethDt, resDkb, resDkw, hstryDkb, hstryDkw)


    // WEB, 엑셀 데이터프레임
    val (dkbWeb, dkwWeb, dkbExcel, dkwExcel) = getViewResCls.getViewRes(ethDt, totalDkb, totalDkw)
    /* [2023.02.01 dk web table 추후 삭제 */
    saveDfsDkCls.saveViewResWeb(ethDt, dkbWeb, dkwWeb, flag)  // WEB 용 데이터를 parquet 으로 저장
    /* [2023.02.01 dk web table 추후 삭제 */
    saveDfsDkCls.saveViewResExcel(ethDt, dkbExcel, dkwExcel, flag)  // EXCEL 용 데이터를 parquet 으로 저장

    // DK TEST 서버 DB 에 결과 업로드, 약 한 시간 정도 소요됨
    val (dkViewResWeb, dkViewResExcel) = getAggDfsDkCls.getDkViewResDfs(ethDt, flag)
    resToDkTestDbCls.runResToDkTestDb(ethDt, dkViewResWeb, dkViewResExcel)  // DK WEB, EXCEL 용 결과 데이터 DB 에 저장
  }
}