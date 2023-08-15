/**
 * 머신러닝 훈련용 파일 생성

 */
package sool.service.item_tagging_bf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{desc, sum}
import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath

class RunGetMlTrainFile(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)

  // base 에서 NAME, ITEM_SZ, SUP_AMT 합계 추출
  def getBaseNmSzAgg(base: DataFrame) = {
    val baseNmSzAgg = base.groupBy("NAME", "ITEM_SZ").agg(sum("SUP_AMT").as("AMT"))
    val baseNmSz = baseNmSzAgg.select('NAME, 'ITEM_SZ)
    (baseNmSzAgg, baseNmSz)
  }

  // 누적사전 에서 NAME, ITEM_SZ 추출
  def getCumulNmSz(cumulNminfoDf: DataFrame) = {
    val cumulNmSz = cumulNminfoDf.filter('SAW =!= "안봄").select('NAME, 'ITEM_SZ).distinct
    cumulNmSz
  }

  // 기존 NAME, ITEM_SZ 데이터프레임, 신규 NAME, ITEM_SZ 데이터프레임 추출
  def getExistingNewNmSzAgg(baseNmSzAgg: DataFrame, baseNmSz: DataFrame, cumulNmSz: DataFrame) = {
    val existingNmSz = baseNmSz.intersect(cumulNmSz)
    val existingNmSzAgg = baseNmSzAgg.join(existingNmSz, Seq("NAME",  "ITEM_SZ")).orderBy(desc("AMT"))

    val newNmSz = baseNmSz.except(cumulNmSz)
    val newNmSzAgg = baseNmSzAgg.join(newNmSz, Seq("NAME", "ITEM_SZ")).orderBy(desc("AMT"))
    (existingNmSzAgg, newNmSzAgg)
  }

  def showInfo(baseNmSzAgg: DataFrame, existingNmSzAgg: DataFrame, newNmSzAgg: DataFrame) = {
    println("총 품목사전 수 : " + baseNmSzAgg.count)
    println("기존 등장(검토완료) 표현 수 : " + existingNmSzAgg.count)
    println("기존 등장(검토미완) + 신규 등장 표현 수 : " + newNmSzAgg.count)
  }

  // 메인
  def runGetMlTrainFiles(ethDt: String, flag: String) = {
    val ethBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)

    /* 필요 데이터 준비 */
    val base = spark.read.parquet(filePathCls.basePath)
    val cumulNminfoDfBf1m = if (fileFuncCls.checkS3FileExist(filePathClsBf1m.cumulNminfoDfNewPath)) {
      spark.read.parquet(filePathClsBf1m.cumulNminfoDfNewPath)  // 누적사전 변경으로 인해 경로 변경 시 사용
    } else {
      spark.read.parquet(filePathClsBf1m.cumulNminfoDfPath)
    }

    val (baseNmSzAgg, baseNmSz) = getBaseNmSzAgg(base)  // base 에서 NAME, ITEM_SZ, SUP_AMT 합계 추출
    val cumulNmSz = getCumulNmSz(cumulNminfoDfBf1m)   // 누적사전 에서 NAME, ITEM_SZ 추출

    /* 기존 NAME, ITEM_SZ 데이터프레임, 신규 NAME, ITEM_SZ 데이터프레임 추출 */
    val (existingNmSzAgg, newNmSzAgg) = getExistingNewNmSzAgg(baseNmSzAgg, baseNmSz, cumulNmSz)

    /* 신규 표현 리스트 저장 --- python 예측 위해 - 월별데이터로 저장 [20200917 수정] */
    fileFuncCls.wParquet(existingNmSzAgg, filePathCls.existedDfPath)  // 기존 NAME, ITEM_SZ 데이터프레임 저장
    fileFuncCls.wParquet(newNmSzAgg, filePathCls.generatedDfPath)  // 신규 NAME, ITEM_SZ 데이터프레임 저장
    fileFuncCls.wParquet(cumulNminfoDfBf1m, filePathCls.beforeDictDfPath)  // 누적사전 beforeDictDF.parquet 으로 저장
    showInfo(baseNmSzAgg, existingNmSzAgg, newNmSzAgg)
  }
}
