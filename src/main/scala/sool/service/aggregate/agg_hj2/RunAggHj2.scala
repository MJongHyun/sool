/**
 * HJ 2차 집계 결과 생성

 */
package sool.service.aggregate.agg_hj2

import org.apache.spark.sql.DataFrame
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath
import sool.service.run.RunService.logger
import sool.service.aggregate.agg_hj2._

class RunAggHj2(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._
  // 집계에 필요한 클래스 선언
  val getAggDfsHj2Cls = new GetAggDfsHj2(spark)
  val getComRgnoHj2Cls = new GetComRgnoHj2(spark)
  val getAnlysMartHj2Cls = new GetAnlysMartHj2(spark)
  val getResHj2Cls = new GetResHj2(spark)
  val saveDfsHj2Cls = new SaveDfsHj2(spark)


  def runAggHj2(ethDt:String, flag:String) = {
    logger.info("[appName=sool] [function=runAggHj2] [runStatus=start] [message=start]")

    // 데이터 준비
    val (martHjb2, martHjs2) = getAggDfsHj2Cls.getMartHj2(ethDt, flag)
    val (comMain, comMainFilSel, territoryOrder) = getAggDfsHj2Cls.getDfsHj2(ethDt, flag)
    val comRgnoHj2 = getComRgnoHj2Cls.getComRgnoHj2Since202210(martHjb2, martHjs2, comMainFilSel, territoryOrder)
    saveDfsHj2Cls.saveComRgnoHj2(comRgnoHj2, ethDt, flag)
    // HJ 2차 분석 마트
    val anlysMartHj2 = getAnlysMartHj2Cls.getAnlysMartHj2(comRgnoHj2, martHjb2, martHjs2)
    saveDfsHj2Cls.saveAnlysMartHj2(anlysMartHj2, ethDt, flag)

    // HJ 2차 결과 마트
    val martResHj2 = getResHj2Cls.getMartResHj2(ethDt, anlysMartHj2, comMain)

    /*
    comResHj2 : HJ 2차 결과 마트에서 업체 정보 추출
    anlysResHj2 : HJ 2차 분석 결과
    */
    val comResHj2 = getResHj2Cls.getComResHj2(martResHj2)
    val anlysResHj2 = getResHj2Cls.getAnlysResHj2(martResHj2)
    saveDfsHj2Cls.saveResHj2Parquet(comResHj2, anlysResHj2, ethDt, flag)  // parquet 저장
    saveDfsHj2Cls.saveResHj2Tsv(comResHj2, anlysResHj2, ethDt, flag)  // tsv 저장

    logger.info("[appName=sool] [function=runAggHj2] [runStatus=end] [message=end]")
  }

  def runAggInv2(ethDt:String, flag:String): Unit ={
    val jdbcGet = new JdbcGet(spark)

    logger.info("[appName=sool] [function=runAggHj2] [runStatus=start] [message=start]")

    // 데이터 준비
    val (martHjb, martHjs) = getAggDfsHj2Cls.getMartHj2(ethDt, flag)

    val (comMain, comMainFilSel, territoryOrder) = getAggDfsHj2Cls.getDfsHj2(ethDt, flag)
    val invPostCd = jdbcGet.getLiquorTbl("HJ_INV_POSTCD")

    val comRgnoHj2 = getComRgnoHj2Cls.getComRgnoInvSince202301(martHjb, martHjs, comMainFilSel, territoryOrder, invPostCd)

    // HJ 2차 분석 마트
    val anlysMartHj2 = getAnlysMartHj2Cls.getAnlysMartHj2(comRgnoHj2, martHjb, martHjs)

    // HJ 2차 결과 마트
    val martResHj2 = getResHj2Cls.getMartResHj2(ethDt, anlysMartHj2, comMain)

    /*
    comResHj2 : HJ 2차 결과 마트에서 업체 정보 추출
    anlysResHj2 : HJ 2차 분석 결과
    */
    val comResHj2 = getResHj2Cls.getComResHj2(martResHj2)
    val anlysResHj2 = getResHj2Cls.getAnlysResHj2(martResHj2)
    saveDfsHj2Cls.saveResHj2Parquet(comResHj2, anlysResHj2, ethDt, flag)  // parquet 저장
    saveDfsHj2Cls.saveResHj2Tsv(comResHj2, anlysResHj2, ethDt, flag)  // tsv 저장

    logger.info("[appName=sool] [function=runAggHj2] [runStatus=end] [message=end]")
  }
}
