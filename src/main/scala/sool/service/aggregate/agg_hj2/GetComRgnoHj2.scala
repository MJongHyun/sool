/**
 * HJ 2차 40000 개 업체 추출

 * */
package sool.service.aggregate.agg_hj2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, when, col}

class GetComRgnoHj2(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  def getHjbBtl500(martHjb2: DataFrame) = {
    val hjbBtl500 = martHjb2.
      filter('VSL_TYPE_CD === "HJBCONT1" && 'VSL_SIZE === "500").
      filter('MFG_CD === "HJBCMPA1")
    val comRgnoHjbBtl500 = hjbBtl500.select('COM_RGNO).distinct
    (hjbBtl500, comRgnoHjbBtl500)
  }

  def getComRgnoNotHjbBtl500(martHjb2:DataFrame, comRgnoHjbBtl500:DataFrame, comMainFilSel:DataFrame, territoryOrder:DataFrame) = {
    val notHjbBtl500 = martHjb2.join(comRgnoHjbBtl500, Seq("COM_RGNO"), "leftAnti")
    val notHjbBtl500Fil = notHjbBtl500.filter('VSL_TYPE_CD === "HJBCONT1" && 'VSL_SIZE === "500")
    val notHjbBtl500G = notHjbBtl500Fil.groupBy('COM_RGNO).agg(sum("SUM_QT").as("Volume"))

    // 주소 및 지역 관련 붙이기
    val notHjbBtl500Addr = notHjbBtl500G.join(comMainFilSel, Seq("COM_RGNO"), "left_outer")
    val notHjbBtl500AddrFil = notHjbBtl500Addr.filter('EXIST === "Y").drop("EXIST")
    val notHjbBtl500AddrTo = notHjbBtl500AddrFil.
      join(territoryOrder, Seq("ADDR1"), "left_outer").
      orderBy('Volume.desc, 'ORDER, 'COM_RGNO)

    val comRgnoNotHjbBtl500 = notHjbBtl500AddrTo.limit(39000).select('COM_RGNO).distinct.toDF
    comRgnoNotHjbBtl500
  }

  def getComRgnoHjbBtl500Ms(martHjb2:DataFrame, hjbBtl500:DataFrame, comMainFilSel:DataFrame, territoryOrder:DataFrame) = {

    val btl500 = martHjb2.filter('VSL_TYPE_CD === "HJBCONT1" && 'VSL_SIZE === "500")

    val btl500Deno = btl500.groupBy('COM_RGNO).agg(sum('SUM_QT) as "DENO")

    val hjbBtl500Nume = hjbBtl500.groupBy('COM_RGNO).agg(sum("SUM_QT") as "NUME")

    val btl500DN = btl500Deno.
      join(hjbBtl500Nume, Seq("COM_RGNO"), "left_outer").
      na.fill(0)

    // 주소 및 지역 관련 붙이기
    val btl500DNAddr = btl500DN.join(comMainFilSel, Seq("COM_RGNO"), "left_outer")
    val btl500DNAddrFil = btl500DNAddr. filter('EXIST === "Y").drop("EXIST")
    val btl500DNAddrTo = btl500DNAddrFil.
      join(territoryOrder, Seq("ADDR1"), "left_outer")

    val hjbBtl500Ms = btl500DNAddrTo.
      withColumn("MS", when('DENO === 0.0, 0.0).otherwise('NUME / 'DENO)).
      filter('MS >= 0.5).
      orderBy('DENO.desc, 'ORDER, 'COM_RGNO)

    // MS 0.5 이상 중에 상위 1000 개 업체 추출
    val comRgnoHjbBtl500Ms = hjbBtl500Ms.limit(1000).select('COM_RGNO).distinct.toDF
    comRgnoHjbBtl500Ms
  }

  /*
  history
  2021년 05월 데이터 집계 때까지 기준
   */
  def getComRgnoHj2Until202105(martHjb2:DataFrame, comMainFilSel:DataFrame, territoryOrder:DataFrame) = {

    val (hjbBtl500, comRgnoHjbBtl500) = getHjbBtl500(martHjb2)

    val comRgnoNotHjbBtl500 = getComRgnoNotHjbBtl500(martHjb2, comRgnoHjbBtl500, comMainFilSel, territoryOrder)

    val comRgnoHjbBtl500Ms = getComRgnoHjbBtl500Ms(martHjb2, hjbBtl500, comMainFilSel, territoryOrder)

    val comRgnoHj2 = comRgnoNotHjbBtl500.union(comRgnoHjbBtl500Ms)
    comRgnoHj2
  }

  /*
  history
  2021년 06월 이후 데이터 집계 기준
   */
  def getComRgnoHj2Since202106(newCom202105Hj2: DataFrame, martHjb2: DataFrame, comMainFilSel: DataFrame) = {
    val hjb2Btl500 = martHjb2.filter('VSL_TYPE_CD === "HJBCONT1" && 'VSL_SIZE === "500")
    val hjb2Btl500Addr = hjb2Btl500.join(comMainFilSel, Seq("COM_RGNO"), "left_outer")
    val hjb2Btl500AddrFltr = hjb2Btl500Addr.filter('EXIST === "Y").drop("EXIST")
    val hj2comSince202106 = hjb2Btl500AddrFltr.join(newCom202105Hj2, Seq("COM_RGNO"))
    val comRgnoHj2 = hj2comSince202106.select('COM_RGNO).distinct
    comRgnoHj2
  }

  /*
  2021년 09월 이후 데이터 집계 기준
   */
  def getComRgnoHj2Since202109(newCom202105Hj2: DataFrame,
                               martHjb2: DataFrame,
                               comMainFilSel: DataFrame,
                               territoryOrder: DataFrame) = {

    val hjb2Btl500 = martHjb2.filter('VSL_TYPE_CD === "HJBCONT1" && 'VSL_SIZE === "500")
    val hjb2Btl500G = hjb2Btl500.groupBy('COM_RGNO).agg(sum("SUM_QT").as("Volume"))
    val hjb2Btl500Addr = hjb2Btl500G.join(comMainFilSel, Seq("COM_RGNO"), "left_outer")
    val hjb2Btl500Fil = hjb2Btl500Addr.filter('EXIST === "Y").drop("EXIST") // comMainFilSel 에 없는 것들 제거
    val hjb2Btl500TrrtryOrdr = hjb2Btl500Fil.join(territoryOrder, Seq("ADDR1"), "left_outer")
    val hjb2Btl500Ordr = hjb2Btl500TrrtryOrdr.orderBy('Volume.desc, 'ORDER, 'COM_RGNO)

    val comRgnoFromNewCom202105Hj2 = hjb2Btl500Ordr.join(newCom202105Hj2, Seq("COM_RGNO"))

    val addComRgnoCnt = (40000 - comRgnoFromNewCom202105Hj2.count()).toInt

    // 최종 결과
    val comRgnoHj2 = if (addComRgnoCnt > 0) {
      val (_, comRgnoHjbBtl500) = getHjbBtl500(martHjb2)
      val comRgnoNotHjbBtl500Pre1 = hjb2Btl500Ordr.join(comRgnoHjbBtl500, Seq("COM_RGNO"), "leftAnti")
      val comRgnoNotHjbBtl500Pre2 = comRgnoNotHjbBtl500Pre1.except(comRgnoFromNewCom202105Hj2)
      val comRgnoNotHjbBtl500 = comRgnoNotHjbBtl500Pre2.orderBy('Volume.desc, 'ORDER, 'COM_RGNO)

      val addComRgnoLmt1 = comRgnoNotHjbBtl500.limit(addComRgnoCnt)
      val addComRgnoLmtLastVol = addComRgnoLmt1.orderBy('Volume.asc, 'ORDER.desc, 'COM_RGNO.desc).first()(2).toString // 역순, 첫 번째 값
      val addComRgnoLmt2Pre = comRgnoNotHjbBtl500.except(addComRgnoLmt1)
      val addComRgnoLmt2 = addComRgnoLmt2Pre.filter('Volume === addComRgnoLmtLastVol).orderBy('ORDER, 'COM_RGNO)
      val addComRgnoLmt = addComRgnoLmt1.union(addComRgnoLmt2)

      comRgnoFromNewCom202105Hj2.union(addComRgnoLmt).select('COM_RGNO).distinct
    } else {
      comRgnoFromNewCom202105Hj2.select('COM_RGNO).distinct
    }
    comRgnoHj2
  }
  /*
  2022년 10월 이후 데이터 집계 기준
   */
  def getComRgnoHj2Since202210(martHjb2: DataFrame,
                               martHjs2: DataFrame,
                               comMainFilSel: DataFrame,
                               territoryOrder: DataFrame) = {
    val domesticLiqMktList = List("B01", "B02", "B03", "B04", "S01", "S02", "S03")
    val domesticLiqMartHjb2 = martHjb2.
      filter(col("MKT_CD").isin(domesticLiqMktList:_*)).
      join(comMainFilSel, Seq("COM_RGNO")).
      join(territoryOrder, Seq("ADDR1"))
    val domesticLiqMartHjs2 = martHjs2.
      filter(col("MKT_CD").isin(domesticLiqMktList:_*)).
      join(comMainFilSel, Seq("COM_RGNO")).
      join(territoryOrder, Seq("ADDR1"))
    val domesticLiqRgnoAmt = domesticLiqMartHjb2.select("COM_RGNO", "ORDER", "SUM_SUP_AMT").
      union(domesticLiqMartHjs2.select("COM_RGNO", "ORDER", "SUM_SUP_AMT"))
    val comRgnoHj2 = domesticLiqRgnoAmt.
      groupBy("COM_RGNO", "ORDER").
      agg(sum(col("SUM_SUP_AMT")) as "AMT").
      orderBy(col("AMT").desc, col("ORDER"), col("COM_RGNO")).
      limit(200000).
      select("COM_RGNO")
    comRgnoHj2
  }

  def getComRgnoInvSince202301(martHjb: DataFrame,
                               martHjs: DataFrame,
                               comMainFilSel: DataFrame,
                               territoryOrder: DataFrame,
                               invPostCd: DataFrame) = {
    val domesticLiqMktList = List("B01", "B02", "B03", "B04", "S01", "S02", "S03")
    val domesticLiqMartHjb = martHjb.
      filter(col("MKT_CD").isin(domesticLiqMktList:_*)).
      join(comMainFilSel, Seq("COM_RGNO")).
      join(territoryOrder, Seq("ADDR1")).
      join(invPostCd, Seq("POST_CD"))
    val domesticLiqMartHjs = martHjs.
      filter(col("MKT_CD").isin(domesticLiqMktList:_*)).
      join(comMainFilSel, Seq("COM_RGNO")).
      join(territoryOrder, Seq("ADDR1")).
      join(invPostCd, Seq("POST_CD"))
    val domesticLiqRgnoAmt = domesticLiqMartHjb.select("COM_RGNO", "ORDER", "SUM_SUP_AMT").
      union(domesticLiqMartHjs.select("COM_RGNO", "ORDER", "SUM_SUP_AMT"))
    val comRgnoHj = domesticLiqRgnoAmt.
      groupBy("COM_RGNO", "ORDER").
      agg(sum(col("SUM_SUP_AMT")) as "AMT").
      orderBy(col("AMT").desc, col("ORDER"), col("COM_RGNO")).
      limit(40000)
    comRgnoHj
  }
}
