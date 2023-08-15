/**

 */
package sool.service.aggregate.agg_dk

import org.apache.spark.sql.{DataFrame, SaveMode}
import sool.common.jdbc.JdbcProp
import sool.service.run.RunService.logger
import java.sql.Statement
import java.util.Properties

class ResToDkTestDb(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  // 필요 클래스 선언
  val jdbcPropCls = new JdbcProp()
  val dkYmCls = new DkYm()

  /* EXCEL 데이터 to DB */
  def resExcelToDbNew(dkYm: String,
                   dkY: String,
                   url: String,
                   dkViewResExcel: DataFrame,
                   stmt: Statement,
                   connProp: Properties) = {

    //  db insert
    val TableName = "DK_RESULT_EXCEL"

    val resExcelYm = dkViewResExcel.filter('YM === dkYm)

    logger.info(s"[file=ResToDkTestDb] [function=resExcelToDb] [status=running] [message=${dkYm} EXCEL 데이터 DK TEST DB 에 저장 시작]")
    resExcelYm.write.mode(SaveMode.Append).jdbc(url, TableName, connProp)
    logger.info(s"[file=ResToDkTestDb] [function=resExcelToDb] [status=running] [message=${dkYm} EXCEL 데이터 DK TEST DB 에 저장 완료]")
  }
  /* WEB 데이터 to DB */
  def resWebToDb(ethDt: String,
                 dkY: String,
                 dkM: String,
                 url: String,
                 dkViewResWeb: DataFrame,
                 stmt: Statement,
                 connProp: Properties) = {
    /* 기존 데이터 삭제 명령 (DK 회계연도 기준으로 새로운 연도가 시작 되는 P01 일 때는 삭제할 필요가 없음) */
    if (dkM != "P01") {
      val qry = s"""DELETE FROM sool_dk_db_dev.DK_RESULT_WEB WHERE FISCAL_YEAR='${dkY}'"""
      stmt.executeUpdate(qry)
    }

    val resWebFiscal = dkViewResWeb.filter('FISCAL_YEAR === dkY)
    logger.info(s"[file=ResToDkTestDb] [function=resWebToDb] [status=running] [message=${dkY} WEB 데이터 DK TEST 서버 DB 에 저장 시작]")
    resWebFiscal.write.mode(SaveMode.Append).jdbc(url, "DK_RESULT_WEB", connProp)
    logger.info(s"[file=ResToDkTestDb] [function=resWebToDb] [status=running] [message=${dkY} WEB 데이터 DK TEST 서버 DB 에 저장 완료]")

    // DK_RESULT_WEB 테이블 내 해당 집계연도의 DK 회계연도 건수
    val dkRsltWbTblCnt = spark.read.jdbc(url, "DK_RESULT_WEB", connProp).filter('FISCAL_YEAR===dkY).count
    val dkViewResWebDkyCnt = dkViewResWeb.filter('FISCAL_YEAR === dkY).count
    logger.info(s"[file=ResToDkTestDb] [function=resWebToDb] [status=running] [message=DK TEST 서버 DB 의 DK_RESULT_WEB 테이블 내 ${dkY} 연도 건수: ${dkRsltWbTblCnt}]")
    logger.info(s"[file=ResToDkTestDb] [function=resWebToDb] [status=running] [message=viewResult_WEB_${ethDt}.parquet 데이터 내 ${dkY} 연도 건수: ${dkViewResWebDkyCnt}]")

    if (dkRsltWbTblCnt != dkViewResWebDkyCnt) {
      logger.warn(s"[file=ResToDkTestDb] [function=resWebToDb] [status=warn] [message=DB 내 테이블 건수와 웹 데이터 건수가 같아야 합니다. 확인이 필요합니다.]")
    }
  }

  /* EXCEL 데이터 to DB */
  def resExcelToDb(ethDt: String,
                   dkYm: String,
                   dkY: String,
                   url: String,
                   dkViewResExcel: DataFrame,
                   connProp: Properties) = {
    val resExcelYm = dkViewResExcel.filter('YM === dkYm)
    logger.info(s"[file=ResToDkTestDb] [function=resExcelToDb] [status=running] [message=${dkYm} EXCEL 데이터 DK TEST 서버 DB 에 저장 시작]")
    resExcelYm.write.mode(SaveMode.Append).jdbc(url, "DK_RESULT_EXCEL", connProp)
    logger.info(s"[file=ResToDkTestDb] [function=resExcelToDb] [status=running] [message=${dkYm} EXCEL 데이터 DK TEST 서버 DB 에 저장 완료]")

    // DK_RESULT_EXCEL 테이블 내 해당 집계연월의 DK 회계연월 건수
    val dkRsltExcelTblCnt = spark.read.jdbc(url, "DK_RESULT_EXCEL", connProp).filter('YM === dkYm).count
    val dkViewResExcelDkyCnt = dkViewResExcel.filter('YM === dkYm).count
    logger.info(s"[file=ResToDkTestDb] [function=resExcelToDb] [status=running] [message=DK TEST 서버 DB 의 DK_RESULT_EXCEL 테이블 내 ${dkY} 연도 건수: ${dkRsltExcelTblCnt}]")
    logger.info(s"[file=ResToDkTestDb] [function=resExcelToDb] [status=running] [message=viewResult_EXCEL_${ethDt}.parquet 데이터 내 ${dkY} 연도 건수: ${dkViewResExcelDkyCnt}]")

    if (dkRsltExcelTblCnt != dkViewResExcelDkyCnt) {
      logger.warn(s"[file=ResToDkTestDb] [function=resExcelToDb] [status=warn] [message=DB 내 테이블 건수와 엑셀 데이터 건수가 같아야 합니다. 확인이 필요합니다.]")
    }
  }

  // 메인
  def runResToDkTestDb(ethDt: String, dkViewResWeb: DataFrame, dkViewResExcel: DataFrame): Unit = {
    val (dkYm, dkY, dkM) = dkYmCls(ethDt)   // DK 회계연도
    val (conn, stmt) = jdbcPropCls.dkTestDbJdbc()   // DK TEST 서버 DB statement
    val (url, connProp) = jdbcPropCls.jdbcPropDkTest()  // DK TEST 서버 DB url, connectionProperties

    logger.info(s"[file=ResToDkTestDb] [function=runResToDkTestDb] [status=start] [message=DK ${dkYm} 데이터 DB 저장 작업 시작]")
    resWebToDb(ethDt, dkY, dkM, url, dkViewResWeb, stmt, connProp)   // WEB 데이터 DB 에 저장
    resExcelToDb(ethDt, dkYm, dkY, url, dkViewResExcel, connProp)   // EXCEL 데이터 DB 에 저장
    dkYmWebToDbNew(dkYm, url, stmt, connProp) // dk 분석연월 DB에 저장
    jdbcPropCls.closeDbJdbc(conn, stmt)
    logger.info(s"[file=ResToDkTestDb] [function=runResToDkTestDb] [status=end] [message=DK ${dkYm} 데이터 DB 저장 작업 완료]")
  }

  def dkYmWebToDbNew(dkYm: String,
                     url: String,
                     stmt: Statement,
                     connProp: Properties) = {
    val TableName = "DK_RESULT_YM"

    logger.info(s"[file=ResToDkTestDb] [function=resWebToDb] [status=running] [message=${dkYm} 분석연월 DK TEST DB 에 저장 시작]")
    val dkYmDf = List(dkYm).toDF("YM")
    val existYm = spark.read.jdbc(url, TableName, connProp)
    if (existYm.filter($"YM"===dkYm).count()==1) {
      logger.info(s"[file=ResToDkTestDb] [function=resWebToDb] [status=running] [message=${dkYm} 분석연월 DB TABLE 내에 존재 ]")
    }
    else{
      dkYmDf.write.mode(SaveMode.Append).jdbc(url, TableName, connProp)
    }
    logger.info(s"[file=ResToDkTestDb] [function=resWebToDb] [status=running] [message=${dkYm} 분석연월 DK TEST DB 에 저장 완료]")
  }

  // 메인
  def runResToDkTestDbNew(ethDt: String, totalDkb: DataFrame, totalDkw: DataFrame): Unit = {
    val (dkYm, dkY, dkM) = dkYmCls(ethDt)   // DK 회계연도
    val (conn, stmt) = jdbcPropCls.dkTestDbJdbc()   // DK TEST 서버 DB statement
    val (url, connProp) = jdbcPropCls.jdbcPropDkTest()  // DK TEST 서버 DB url, connectionProperties

    val dkViewResWeb = totalDkb.union(totalDkw)
    logger.info(s"[file=ResToDkTestDb] [function=runResToDkTestDb] [status=start] [message=DK ${dkYm} 데이터 DB 저장 작업 시작]")
    resExcelToDbNew(dkYm, dkY, url, dkViewResWeb, stmt, connProp)   // EXCEL 데이터 DB 에 저장
    dkYmWebToDbNew(dkYm, url, stmt, connProp) // dk 분석연월 DB에 저장
    jdbcPropCls.closeDbJdbc(conn, stmt)
    logger.info(s"[file=ResToDkTestDb] [function=runResToDkTestDb] [status=end] [message=DK ${dkYm} 데이터 DB 저장 작업 완료]")
  }
}