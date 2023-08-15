/**
 * base 데이터 생성

 */
package sool.extract.extract_data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, udf}
import sool.common.function.FileFunc
import sool.common.path.FilePath

class ExtrctBase(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  /* 필요 클래스 선언 */
  val fileFuncCls = new FileFunc(spark)

  def getItemExtrctUdf() = udf((rawItem: String) => {
    val itemList = rawItem.split(";")
    val itemListLength = itemList.size
    val barcodeRegex = "\\d{10,}[:;,]"
    val numberExceptRegex = "[^0-9]".r
    val codeRegex = "((1|2|3|4|5|6|7|8|9|01|02|03|04|05|06|07|08|09|10|11|12|13|)[:;,]){1,}"
    val removeRegex = (barcodeRegex+"|"+codeRegex).r

    rawItem match{
      case a if (itemListLength == 3) => itemList(2)
      case b if (itemListLength == 4) => itemList(3)
      case _ => removeRegex.replaceAllIn(rawItem.
        replace("-","").
        replace(" ", "").
        replace("\u3000", ""), "//"
      ).split("//").
        filter(s=>s != "").
        filter(s=>numberExceptRegex.findAllIn(s).size != 0).
        toList(0)
    }
  })

  def getExtrctNameUdf() = udf((rawItem: String) => {
    val braRegex = """\[|\{|\<""".r
    val ketRegex = """\]|\}|\>""".r
    val replaceBracketItem = ketRegex.replaceAllIn(braRegex.replaceAllIn(rawItem, "("), ")")
    val globalKeyRegex = """[★☆▲]"""
    val specialKeyRegex = """[!#$*/@_\+~`^&\-=?\\]+"""
    val RFIDRegex = """\(?RFID\)?"""
    val ethanolKey1Regex = """\(?(유흥|가정|일반|할인|면세)용?\)?"""
    val ethanolKey2Regex = """\([유|가|일|할]\)"""
    val whiteSpaceRegex = """^\s+|\s+$"""
    val removeRegex = (globalKeyRegex+"|"+specialKeyRegex+"|"+RFIDRegex+"|"+ethanolKey1Regex+"|"+ethanolKey2Regex+"|"+whiteSpaceRegex).r
    val removeKeywordItem = removeRegex.replaceAllIn(replaceBracketItem, "")
    var item = removeKeywordItem
    val replaceList = List(
      ("˚", "도"),("㎖", "ML"),("ℓ", "L"),
      ("入", "입"),("本", "본"), ("大", "대"),
      ("中", "중"),("小", "소"),("紙", "지"),
      ("生", "생"), ("\u3000", ""), ("RFID", "")
    )
    replaceList.foreach(s=>{
      item = item.replace(s._1, s._2)
    })
    val whiteSpaceReplaceRegex = "\\s+"
    val item2 = whiteSpaceReplaceRegex.r.replaceAllIn(item, " ").toUpperCase
    val item3 = if (item2.size != 0) item2 else null

    val specialKeyRegex1 = """^['"():!#$*./@_\+.~`%^&\-=?\\]+""" // 시작 특수문자 제거
    val specialKeyRegex2 = """['"()!#$*/@_\+~`^&\-=?\\]+""" // . % 남기기 (참이슬 후레쉬, 16.9도 키워드)
    val removRegex = (specialKeyRegex1+"|"+specialKeyRegex2).r
    val LastRegex = removRegex.replaceAllIn(item3, "")
    LastRegex.replace("RFID","").replace(" ","") // RFID 제거
  })

  def getExtrctBarcodeUdf() = udf((rawItem: String) => {
    // val barcodeRegex = "\\d{10,}[:;,]".r
    val barcodeRegex = "\\d{13}[:;,]".r
    val barcodeList = barcodeRegex.findAllIn(rawItem.
      replace(" ", "").
      replace("\t", "").
      replace("-", "")).
      toList.
      map(
        s => s.
          replace(";", "").
          replace(":", "").
          replace(",", "")
      )
    if ((barcodeList.mkString(" ")).size > 0) "R"+barcodeList.mkString(" ") else null
  })

  def getExtrctTypeUdf() = udf((rawItem: String) => {
    var rawItem2 = if (rawItem.contains(";") == false) rawItem.replace(":", ";") else rawItem
    val barcodeRegex = "\\d{3,}[:;,]".r
    val codeRegex = "(\\d{1,2}[;])".r

    val parseItem = codeRegex.findAllIn(
      barcodeRegex.replaceAllIn(
        rawItem2.replace(" ", "").replace("\t", ""), ""
      )
    ).toList
    val itemLength = parseItem.size
    val kindList = List("01","02","03","04","05","06","07","08","09","10","11","12","13")

    itemLength match {
      case a if (itemLength == 0) => null
      case _ => {
        var kind = parseItem(itemLength-1).replace(";", "")
        kind = if (kind.size == 1) "0"+kind else kind
        kind match {
          case b if (kindList.contains(kind)) => kind
          case _ => null
        }
      }
    }
  })

  // 1000L-> 1000000.0(201810, 201811), 60Keg -> 60(201811, 201901)
  def getNormalizeSizeUdf() = udf((rawSize: String) => {
    val upperRawSize = rawSize.toUpperCase.
      replace(" ", "").
      replace("리", "L").
      replace(",", ".").
      replace("ℓ", "L") // .replace(",", ".") 제거
    val doContainLiter = upperRawSize.matches(".*\\d*\\.{0,1}\\d+L.*")
    val floatRegex = """[0-9]*\.?[0-9]+""".r
    val multiRegex = """\*[0-9]+""".r
    val minusRegex = """-[0-9]+""".r

    val floatList = floatRegex.findAllIn(minusRegex.replaceAllIn(multiRegex.replaceAllIn(upperRawSize, ""), "")).toList

    if (floatList.size == 0) {
      null
    } else {
      var itemSz = floatList(0).toDouble
      if (doContainLiter || itemSz <= 30) {
        (itemSz * 1000.0).toString
      } else {
        itemSz.toString
      }
    }
  })

  def getNormalizeQtUdf() = udf((rawQt: String, rawAmt: Double) => {
    val qtNum = "[^0-9\\.]".r.replaceAllIn(rawQt, "")
    qtNum match {
      case a if (rawAmt < 0) => (math.abs(qtNum.toDouble) * -1.0).toString
      case _ => (math.abs(qtNum.toDouble)).toString
    }
  })

  def getNormalizeUpUdf() = udf((rawUp : Double) => {
    math.abs(rawUp)
  })

  /*
  알 수 없는 주석:
  [2020.10.05] 함수 제외 (필요없는 함수 제거)
  val extractYM = udf((date: String)=>{date.slice(0, 6)})
  val extrctDay = udf((date: String)=>{date.slice(6, 8)})
   */

  /* base 추출: 파싱, 전처리 작업 진행 */
  def getBase(dtiCh3: DataFrame) = {
    val normalizeSizeUdf = getNormalizeSizeUdf()
    val normalizeQtUdf = getNormalizeQtUdf()
    val normalizeUpUdf = getNormalizeUpUdf()
    val itemExtrctUdf = getItemExtrctUdf()
    val extrctNameUdf = getExtrctNameUdf()
    val extrctBarcodeUdf = getExtrctBarcodeUdf()
    val extrctTypeUdf = getExtrctTypeUdf()

    val dtiCh3AddCols = dtiCh3.
      withColumn("CHANNEL", lit("03")).
      withColumn("ITEM_SZ", normalizeSizeUdf('ITEM_SZ)).
      withColumn("ITEM_QT", normalizeQtUdf('ITEM_QT, 'SUP_AMT)).
      withColumn("ITEM_UP", normalizeUpUdf('ITEM_UP)).
      withColumn("NAME", extrctNameUdf(itemExtrctUdf('ITEM_NM))).
      withColumn("BARCODE", extrctBarcodeUdf('ITEM_NM)).
      withColumn("TYPE", extrctTypeUdf('ITEM_NM))

    val dtiCh3AddColsFltr = dtiCh3AddCols.filter('ITEM_SZ.isNotNull)

    val base = dtiCh3AddColsFltr.select(
      "ISS_ID", "ITEM_NO", "SUP_RGNO", "SUP_NM", "SUP_ADDR",
      "BYR_RGNO", "BYR_NM", "BYR_ADDR", "ITEM_NM", "ITEM_SZ",
      "ITEM_QT", "ITEM_UP", "SUP_AMT", "WR_DT", "DATE",
      "NAME", "BARCODE", "CHANNEL", "TYPE", "TAX_AMT"
    )
    base
  }

  /* 메인 */
  def extrctBase(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)

    /* base 추출 */
    // 소매상 리스트 데이터 조인과정 제거 [2021.01.04]
    val dtiCh3 = fileFuncCls.rParquet(filePathCls.dtiCh3Path)
    val base = getBase(dtiCh3)
    fileFuncCls.wParquet(base, filePathCls.basePath)    // 저장
  }
}
