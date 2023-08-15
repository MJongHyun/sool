package sool.extract.extract_data

import org.apache.spark.sql.functions.{lit, udf}
import sool.common.function.{FileFunc, GetTime}
import sool.common.path.FilePath
import org.apache.spark.sql.DataFrame

// Zeppelin 1.Extract 1-0 Check NonEthanol Com - 이 클래스를 실행하여 parquet 파일 업데이트}
class NonEthComFilter(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._
  val getTimeCls = new GetTime()
  val fileFuncCls = new FileFunc(spark)

  val itemUDF = udf((item: String)=> {
    if (("(\\d{0,2}[:;]){2,}".r.findAllIn(item).size > 0) || ("\\d{10,};".r.findAllIn(item).size > 0)) "Y" else "N"
  })

  // 비주류 업체리스트의 사업자 번호 비주류 아이템 업데이트
  def rgnoReBulid(ethanolItem: DataFrame, rgnoDf: DataFrame, nonEthanolCom: DataFrame, nonEthanolCh2: DataFrame) = {
    var removeDfFilterPre = nonEthanolCom
    val removeRgno = nonEthanolCh2.join(ethanolItem, Seq("ITEM_NM")).select("SUP_RGNO").distinct()
    if (removeRgno.count == 0) {
      removeDfFilterPre = nonEthanolCom
    } else {
      removeDfFilterPre = nonEthanolCom.except(removeRgno)
    }
    var removeDfFilter = removeDfFilterPre
    if (rgnoDf.filter($"SUP_RGNO" === "" || $"SUP_RGNO" === "0").count > 0) {
      removeDfFilter = removeDfFilterPre
    } else {
      removeDfFilter = removeDfFilterPre.union(rgnoDf)
    }
    removeDfFilter
  }

  // 비주류 업체 아이템 리스트의 사업자 번호 비주류 아이템 업데이트
  def itemReBulid(ethDealItem: DataFrame, rgnoDf: DataFrame, nonEthItem: DataFrame, dtiDF: DataFrame) = {
    var nonItemResPre = nonEthItem
    val newRgnoItem = dtiDF.join(rgnoDf, Seq("SUP_RGNO")).select("ITEM_NM").distinct()
    if (newRgnoItem.count == 0) {
      nonItemResPre = nonEthItem
    } else {
      nonItemResPre = nonEthItem.union(newRgnoItem)
    }
    val nonItemResTotal = nonItemResPre.union(ethDealItem)
    nonItemResTotal
  }

  //main - 비주류업체 체크
  def nonEthaWrite(ethDt: String, rmItemNm: String, addRgno: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)

    val removeDF = fileFuncCls.rParquet(filePathCls.baseCmmnNonEthComPath).select("SUP_RGNO")
    val nonEthItem = fileFuncCls.rParquet(filePathCls.nonCmmnEthItemPath)
    val dtiDF = fileFuncCls.rParquet(filePathCls.dtiPath)

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

    val rgnoDf = addRgno.split(',').toList.toDF("SUP_RGNO")
    val ethanolItem = rmItemNm.split(',').toList.toDF("ITEM_NM")
    rgnoDf.show(2, false)
    ethanolItem.show(2, false)

    // 사업자, 아이템으로 비주류업체 리스트 parquet 파일 업데이트 하는 코드.
    val rgnoResult = rgnoReBulid(ethanolItem, rgnoDf, removeDF, nonEthanolCh2)
    fileFuncCls.wParquet(rgnoResult.distinct(), filePathCls.nonEthComPath)
    val rgnoResultCopy = fileFuncCls.rParquet(filePathCls.nonEthComPath)
    fileFuncCls.owParquet(rgnoResultCopy.distinct() ,filePathCls.baseCmmnNonEthComPath)

    // 사업자, 아이템으로 비주류업체 아이템 리스트 parquet 파일 업데이트 하는 코드.
    val ethDealItem = nonItemRes.except(ethanolItem).toDF()
    ethDealItem.show(10, false)
    val itemResult = itemReBulid(ethDealItem, rgnoDf, nonEthItem, dtiDF)
    itemResult.show(10, false)
    fileFuncCls.wParquet(itemResult, filePathCls.nonEthItemPath)
    val itemResultCopy = spark.read.parquet(filePathCls.nonEthItemPath)
    fileFuncCls.owParquet(itemResultCopy.distinct() ,filePathCls.nonCmmnEthItemPath)
  }
}