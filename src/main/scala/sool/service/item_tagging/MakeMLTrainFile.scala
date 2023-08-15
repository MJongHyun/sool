package sool.service.item_tagging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, lit}
import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Zeppelin 2.Refine 2-1 Machine Tagging Train File
class MakeMLTrainFile(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._
  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)

  def showInfo(nameSizeAggDF: DataFrame, generatedDF: DataFrame) = {
    println("총 품목사전 수 : " + nameSizeAggDF.count)
    println("기존 등장(검토완료) 표현 수 : " + lit(nameSizeAggDF.count - generatedDF.filter($"SAW" === "안봄").count))
    println("기존 등장(검토미완) + 신규 등장 표현 수 : " + generatedDF.filter($"SAW"==="안봄").count)
  }

  def getBaseNmSzAgg(base: DataFrame) = {
    val baseNmSzAgg = base.groupBy("NAME", "ITEM_SZ").agg(sum("SUP_AMT").as("AMT"))
    (baseNmSzAgg)
  }

  def getAppearCnt(baseCurrent: DataFrame)={
    val barWindow = Window.partitionBy('NAME, 'ITEM_SZ).orderBy('count.desc, 'BARCODE)
    val barcodeRes = baseCurrent.
      na.drop(Seq("BARCODE")).
      select('BARCODE, 'SUP_RGNO, 'NAME, 'ITEM_SZ).distinct().
      groupBy("NAME", "ITEM_SZ", "BARCODE").count.
      withColumn("RANK", dense_rank().over(barWindow)).
      filter('RANK===1).drop("RANK", "count").
      select("NAME", "ITEM_SZ", "BARCODE")
    // 주종은 NAME, ITEM_SZ 건수가 많은 값을 올림 (03은 제외, 유흥채널일 수도 있어서 제외함)
    val typeWindow = Window.partitionBy('NAME, 'ITEM_SZ).orderBy('count.desc, 'Type)
    val typeRes = baseCurrent.
      na.drop(Seq("TYPE")).
      select('TYPE, 'SUP_RGNO, 'NAME, 'ITEM_SZ).distinct().
      groupBy("NAME", "ITEM_SZ", "TYPE").count.
      filter($"TYPE" =!= "03").
      withColumn("RANK", dense_rank().over(typeWindow)).
      filter('RANK===1).drop("RANK", "count").
      select("NAME", "ITEM_SZ", "TYPE")
    // 단가는 NAME, ITEM_SZ의 평균 단가로 추출
    val upRes = baseCurrent.
      groupBy($"NAME", $"ITEM_SZ").
      agg(mean($"ITEM_UP") as "ITEM_UP")
    (barcodeRes, typeRes, upRes)
  }

  // 메인
  def runGetMlTrainFiles(ethDt: String, flag: String) = {
    val ethBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)
    val fileFuncCls = new FileFunc(spark)

    // [2021.09.14] 코드전면 수정
    // 전달 누적사전, 이번달 유흥데이터 추출
    val beforeDictInfo = fileFuncCls.rParquet(filePathClsBf1m.cumulNminfoDfNewPath)
    val baseCurrent = fileFuncCls.rParquet(filePathCls.basePath)

    // 이번달 유흥데이터에서 NAME, ITEM_SZ로 매입한 금액 추출
    val nameSizeAggDF = getBaseNmSzAgg(baseCurrent)    // base 에서 NAME, ITEM_SZ, SUP_AMT 합계 추출

    // 전달에 있던 NAME, ITEM_SZ와 신규로 나온 NAME, ITEM_SZ 구분
    val intersectNameSz = nameSizeAggDF.select("NAME", "ITEM_SZ").intersect(beforeDictInfo.select("NAME", "ITEM_SZ"))
    val exceptNameSz = nameSizeAggDF.select("NAME", "ITEM_SZ").except(beforeDictInfo.select("NAME", "ITEM_SZ"))

    // 바코드는 NAME, ITEM_SZ 건수가 많은 값을 올림
    val (barcodeRes, typeRes,upRes) = getAppearCnt(baseCurrent)

    // 전달에 있던 값들도 업데이트해서 값 추출
    val bfNameSizeAggDFInfo = nameSizeAggDF.
      join(intersectNameSz, Seq("NAME", "ITEM_SZ")).
      join(beforeDictInfo.select("WR_DT", "NAME", "ITEM_SZ", "ITEM", "SAW"), Seq("NAME", "ITEM_SZ")).
      join(upRes, Seq("NAME", "ITEM_SZ")).
      join(typeRes, Seq("NAME", "ITEM_SZ"), "left_outer").
      join(barcodeRes, Seq("NAME", "ITEM_SZ"), "left_outer").
      na.fill("").
      select('WR_DT, 'BARCODE, 'TYPE, 'NAME, 'ITEM_SZ, 'ITEM_UP, 'AMT, 'SAW)
    // 신규 값은 NAME, ITEM_SZ 바코드, 주종, 평균 단가 값 추출
    val newNameSizeAggDFInfo = nameSizeAggDF.
      join(exceptNameSz, Seq("NAME", "ITEM_SZ"))

    // 위에서 구한 값을 바탕으로 신규 값에 대한 데이터 추출
    val newItemRes = newNameSizeAggDFInfo.
      join(upRes, Seq("NAME", "ITEM_SZ")).
      join(typeRes, Seq("NAME", "ITEM_SZ"), "left_outer").
      join(barcodeRes, Seq("NAME", "ITEM_SZ"), "left_outer").
      na.fill("").
      withColumn("WR_DT", lit(ethDt)).
      withColumn("SAW", lit("안봄")).
      select('WR_DT, 'BARCODE, 'TYPE, 'NAME, 'ITEM_SZ, 'ITEM_UP, 'AMT, 'SAW)
    // beforeDictDF : 전달 누적사전에서 본 값만 가져와서 추출, generatedDF : 이번달에 나온 NAME, ITEM_SZ 가져와서 추출
    val generatedDF = newItemRes.union(bfNameSizeAggDFInfo)
    val beforeDictDF = beforeDictInfo.filter($"SAW" =!= "안봄")

    // ### 신규 표현 리스트 저장 --- python 예측 위해 - 월별데이터로 저장 [20200917 수정]
    generatedDF.repartition(1).write.parquet(filePathCls.generatedDfPath)
    beforeDictDF.repartition(1).write.parquet(filePathCls.beforeDictDfPath)
    showInfo(nameSizeAggDF, generatedDF)
  }
}
