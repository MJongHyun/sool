package sool.extract.extract_data

import org.apache.spark.sql.functions.{lit, udf}
import sool.common.function.FileFunc
import sool.common.path.FilePath

// [2022.10.27]
// Zeppelin 1.Extract 1-1 Extract Ethanol
class ExtractEthanol(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  /* 필요 클래스 선언 */
  val fileFuncCls = new FileFunc(spark)


  def supFilterByEthConv: (String => Boolean) = { item_nm =>
    // 구분자 개수 필터 및 바코드 유무 검증
    val semicol_com = raw";".r
    val colon_com = raw":".r

    // 구분자(;)의 기본 갯수는 3개 경우에 따라서 1개만 사용하는 업체가 있고, 주세법표시방식을 두번 사용하는 업체도 있기때문에 0<n(';')<7 인 것만 주류 아이템으로 가정
    val is_semicolon = (semicol_com.findAllIn(item_nm).toList.length > 0) && (semicol_com.findAllIn(item_nm).toList.length < 7)
    // 구분자(:)의 기본 갯수는 3개 경우에 따라서 2개만 사용하는 업체가 있고, 주세법표시방식이 있기때문에 1<n(':')<4 인 것만 주류 아이템으로 가정
    val is_colon = (colon_com.findAllIn(item_nm).toList.length > 1) && (colon_com.findAllIn(item_nm).toList.length < 4)
    var sep = ""
    if (is_semicolon) {
      // 첫번째 적힌게 (바코드)숫자인지 T/F
      sep = ";"
      val sepRegex = "^" + sep
      val is_barcodeRegex = "^[^" + sep + "]*"
      val is_barcode = is_barcodeRegex.r.findAllIn(item_nm).toList(0)

      val NofNumeric = "\\d".r.findAllIn(is_barcode).toList.size
      val NofNonNum = "[^\\d]".r.findAllIn(is_barcode).toList.size

      val is_greater_numeric = (NofNumeric * 0.3 > NofNonNum) && (NofNumeric >= 10)
      val is_not_barcode = sepRegex.r.findAllIn(item_nm).size > 0

      if (is_not_barcode) true
      else if (is_greater_numeric) true
      else false
    }
    else if (is_colon) {
      // 첫번째 적힌게 (바코드)숫자인지 T/F
      sep = ":"
      val sepRegex = "^" + sep
      val is_barcodeRegex = "^[^" + sep + "]*"
      val is_barcode = is_barcodeRegex.r.findAllIn(item_nm).toList(0)

      val NofNumeric = "\\d".r.findAllIn(is_barcode).toList.size
      val NofNonNum = "[^\\d]".r.findAllIn(is_barcode).toList.size

      val is_greater_numeric = (NofNumeric * 0.3 > NofNonNum) && (NofNumeric >= 10)
      val is_not_barcode = sepRegex.r.findAllIn(item_nm).size > 0

      if (is_not_barcode) true
      else if (is_greater_numeric) true
      else false
    }
    else {
      false
    }
  }

  // 주세법 기준 T/F
  def supFilterByFixedEthConv: (String => Boolean) = { item_raw =>
    // 아이템명에 띄어쓰기 제거
    val item_nm = item_raw.replace(" ","")
    // regex1,2 - 주세법 기준으로 잘 적은 경우
    // regex3,4 - 바코드를 제외하고 주세법으로 잘 적은 경우
    val ethItemRegex1 = "^\\s*\\d{10,}\\s*;\\s*\\d{0,2}\\s*;\\s*\\d{2}\\s*(;|[^\\d])"
    val ethItemRegex2 = "^\\s*\\d{10,}\\s*:\\s*\\d{0,2}\\s*:\\s*\\d{2}\\s*(:|[^\\d])"
    val ethItemRegex3 = "^;\\s*\\d{2}\\s*;\\s*(\\s*|\\d{2})\\s*(;|[^\\d])"
    val ethItemRegex4 = "^:\\s*\\d{2}\\s*:\\s*(\\s*|\\d{2})\\s*(:|[^\\d])"
    val is_ethItem = (ethItemRegex1.r.findAllIn(item_nm).size > 0) || (ethItemRegex2.r.findAllIn(item_nm).size > 0) || (ethItemRegex3.r.findAllIn(item_nm).size > 0) || (ethItemRegex4.r.findAllIn(item_nm).size > 0)

    if(is_ethItem) true
    else false
  }

  def extractEthanolData(ethDt: String, flag: String): Unit = {
    val filePathCls = new FilePath(ethDt, flag)
    // 구분자 개수에 따라 바코드 검증
    val ethUdf = udf(supFilterByEthConv)
    // 주세법 검증
    val ethUdf2 = udf(supFilterByFixedEthConv)
    // 숫자;숫자; or 숫자 10자리 이상이면 Y (기존 검증)
    val itemUdf = udf((item: String)=> {
      if (("(\\d{0,2}[:;]){2,}".r.findAllIn(item).size > 0) || ("\\d{10,};".r.findAllIn(item).size > 0)) "Y" else "N"
    })

    val dti = fileFuncCls.rParquet(filePathCls.dtiPath)

    // 조건 1로 주류 데이터 추출
    val dtiTemp = dti.
      filter('ITEM_NM.isNotNull).
      filter('ITEM_SZ.isNotNull).
      filter('ITEM_UP.isNotNull).
      filter('ITEM_QT.isNotNull).
      filter('ITEM_QT=!=0.0).
      filter('SUP_AMT =!= 0.0).
      withColumn("TF",ethUdf($"ITEM_NM")).
      groupBy('WR_DT, 'SUP_RGNO).
      pivot('TF).count().
      na.fill(0).withColumn("ratio",'true/('true+'false))

    val ethY1 = dtiTemp.filter($"ratio" >= 0.5).select("SUP_RGNO")
    // 조건 2로 주류 데이터 추출
    val tmpRgno = dtiTemp.filter($"ratio" < 0.5).select("SUP_RGNO")
    val ethY2 = dti.
      join(tmpRgno, Seq("SUP_RGNO")).
      filter('ITEM_NM.isNotNull).
      filter('ITEM_SZ.isNotNull).
      filter('ITEM_UP.isNotNull).
      filter('ITEM_QT.isNotNull).
      filter('ITEM_QT=!=0.0).
      filter('SUP_AMT =!= 0.0).
      withColumn("TF",ethUdf2($"ITEM_NM")).
      groupBy('WR_DT, 'SUP_RGNO).
      pivot('TF).count().
      na.fill(0).filter($"true" > 0).select("SUP_RGNO")
    // 기존 조건으로 주류 아이템 추출
    val ethY = ethY1.union(ethY2)
    val dtiEthanol = dti.
      join(ethY, Seq("SUP_RGNO")).
      filter('ITEM_NM.isNotNull).
      filter('ITEM_SZ.isNotNull).
      filter('ITEM_UP.isNotNull).
      filter('ITEM_QT.isNotNull).
      filter('ITEM_QT=!=0.0).
      filter('SUP_AMT =!= 0.0).
      withColumn("ETHANOL", itemUdf('ITEM_NM)).
      filter('ETHANOL === "Y").
      drop("ETHANOL")

    fileFuncCls.wParquet(dtiEthanol, filePathCls.dtiEthPath)
  }

  def extractEthanolDataInv(ethDt: String, flag: String): Unit = {
    val filePathCls = new FilePath(ethDt, flag)
    // 주류데이터 추출 함수

    val itemUdf = udf((item: String)=> {
      if (("(\\d{0,2}[:;]){2,}".r.findAllIn(item).size > 0) || ("\\d{10,};".r.findAllIn(item).size > 0)) "Y" else "N"
    })
    ////  주류데이터 추출 1단계
    // 비주류 업체 리스트
    val removeDF = fileFuncCls.rParquet(filePathCls.baseCmmnNonEthComPath).select("SUP_RGNO").withColumn("ETHANOLYN", lit("N"))

    // 품목명, 용량, 단가, 수량 NotNull + 공급가액 0아닌 것 + 비주류 업.체 joinde
    val dtiTemp = fileFuncCls.rParquet(filePathCls.dtiPath).
      filter('ITEM_NM.isNotNull).
      filter('ITEM_SZ.isNotNull).
      filter('ITEM_UP.isNotNull).
      filter('ITEM_QT.isNotNull).
      filter('ITEM_QT =!= 0.0).
      filter('SUP_AMT =!= 0.0).
      // filter('SUP_RGNO =!= "").
      join(removeDF, Seq("SUP_RGNO"), "left_outer")

    ////  주류데이터 추출 2단계
    // 주류데이터 저장
    val dtiEthanol = dtiTemp.filter('ETHANOLYN.isNull).drop('ETHANOLYN).
      withColumn("ETHANOL", itemUdf('ITEM_NM)).filter('ETHANOL === "Y").drop("ETHANOL")

    fileFuncCls.wParquet(dtiEthanol, filePathCls.dtiEthPath)
  }
}


