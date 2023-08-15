package sool.extract.extract_data

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, udf}
import sool.common.function.{FileFunc, GetTime}
import sool.common.path.FilePath
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import sool.address.run.RunComMain.getTimeCls

// Zeppelin 1.Extract 1-0 Check NonEthanol Com - 이 클래스를 실행 후 결과 값 확인하여 다음 클래스 실행.
class CheckNonEthCom(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._
  // 필요 클래스 선언
  val getTimeCls = new GetTime()
  val fileFuncCls = new FileFunc(spark)

  val itemUDF = udf((item: String)=> {
    if (("(\\d{0,2}[:;]){2,}".r.findAllIn(item).size > 0) || ("\\d{10,};".r.findAllIn(item).size > 0)) "Y" else "N"
  })

  // 비주류업체 리스트의 공급거래 검토 - 주류 거래를 하는지 확인
  def nonethanol_sup_cheak(removeDF: DataFrame, nonEthItem: DataFrame, dtiDF: DataFrame) = {
    val nonEthanolCh2 = dtiDF.
      join(removeDF, Seq("SUP_RGNO")).// 비주류업체리스트의 공급거래
      join(nonEthItem, Seq("ITEM_NM"), "leftAnti"). // 비주류업체가 거래한 아이템리스트
      filter('ITEM_NM.isNotNull).
      filter('ITEM_SZ.isNotNull).
      filter('ITEM_UP.isNotNull).
      filter('ITEM_QT.isNotNull).
      filter('ITEM_QT=!=0.0).
      filter('SUP_AMT =!= 0.0).
      filter(itemUDF('ITEM_NM) === "Y") // 주세법 형식을 지키는 거래

    val nonItemRes = nonEthanolCh2.select("ITEM_NM").distinct()
    nonItemRes.show(100, false)
  }

  // 신규업체 중 비주류 거래 확인.
  def nonetha_trade_cheak(bfDtiEth: DataFrame, dtiDF: DataFrame, removeDF: DataFrame) = {
    val wITEM_NM = Window.partitionBy("SUP_RGNO").orderBy("ITEM_NM")

    val dtiEthanolPre = dtiDF.
      filter('ITEM_NM.isNotNull).
      filter('ITEM_SZ.isNotNull).
      filter('ITEM_UP.isNotNull).
      filter('ITEM_QT.isNotNull).
      filter('ITEM_QT=!=0.0).
      filter('SUP_AMT =!= 0.0).
      join(removeDF, Seq("SUP_RGNO"), "leftAnti").
      withColumn("ETHANOL", itemUDF('ITEM_NM)).
      filter('ETHANOL === "Y").
      drop("ETHANOL")

    val newSupRgno = dtiEthanolPre.select("SUP_RGNO").distinct().except(bfDtiEth.select("SUP_RGNO").distinct())

    val newSupItemRes = dtiEthanolPre.join(newSupRgno, Seq("SUP_RGNO")).
      select("SUP_RGNO", "ITEM_NM").distinct().
      withColumn("RANK", rank.over(wITEM_NM)).filter($"Rank" <= 3).
      groupBy($"SUP_RGNO").
      agg(collect_list("ITEM_NM") as "ITEM").
      withColumn("ITEM", concat_ws(", ", $"ITEM")).
      select("SUP_RGNO", "ITEM")

    newSupItemRes.show(100, false)
  }
  //main - 비주류업체 체크
  def nonEthaResult(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val ethDtBf1m = getTimeCls.getEthBf1m(ethDt)
    val filePathClsBf1m = new FilePath(ethDtBf1m, flag)

    val bfDtiEth = fileFuncCls.rParquet(filePathClsBf1m.dtiEthPath)
    val removeDF = fileFuncCls.rParquet(filePathCls.baseCmmnNonEthComPath).select("SUP_RGNO")
    val nonEthItem = fileFuncCls.rParquet(filePathCls.nonCmmnEthItemPath)
    val dtiDF = fileFuncCls.rParquet(filePathCls.dtiPath)

    println("비주류 업체 거래 리스트 (주류 거래가 존재하는지 확인 필요)")
    nonethanol_sup_cheak(removeDF, nonEthItem, dtiDF)
    println("신규 업체 거래 리스트 (비주류 거래가 존재하는지 확인 필요)")
    nonetha_trade_cheak(bfDtiEth, dtiDF, removeDF)
  }
}