/**
 * 1) EU 단위 변환, 백만 단위 변환, 반올림, 마스킹, DK 회계 연월 변환
 * 2) 해당 집계 연월 데이터 + 과거 히스토리 데이터

 */
package sool.service.aggregate.agg_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, round, udf, when}
import org.apache.spark.sql.types.DecimalType
import java.time._
import java.time.format.DateTimeFormatter

class GetTotalDk(spark: org.apache.spark.sql.SparkSession) extends Serializable {
  import spark.implicits._

  // 필요 클래스 선언
  val dkYmCls = new DkYm()
  val getViewResCls = new GetViewRes(spark)


  // QT 를 EU 단위로 변환
  def trnsfQtToEu(beerDf: DataFrame, whiskyDf: DataFrame) = {
    val beerQtToEuUdf = udf((qt: Double) => qt / 90000)   // 맥주는 90000ml 당 1EU
    val whiskyQtToEuUdf = udf((qt: Double) => qt / 9000)  // 위스키는 9000ml 당 1EU

    val beerEuDf = beerDf.withColumn("QT_MARKET", beerQtToEuUdf('QT_MARKET))
    val whiskyEuDf = whiskyDf.withColumn("QT_MARKET", whiskyQtToEuUdf('QT_MARKET))
    (beerEuDf, whiskyEuDf)
  }

  // AMT 를 백만 단위로 변환
  def trnsfAmtToUnit(beerDf: DataFrame, whiskyDf: DataFrame) = {
    val trgtUnit = 1000000
    val beerUnitDf = beerDf.withColumn("AMT_MARKET", 'AMT_MARKET / trgtUnit)
    val whiskyUnitDf = whiskyDf.withColumn("AMT_MARKET", 'AMT_MARKET / trgtUnit)
    (beerUnitDf, whiskyUnitDf)
  }

  //소수점 절사 (2자리 -> 3자리에서 반올림)
  def trimD(colVal: String) = {
    //roundingPoint : 소수점 몇자리까지 나타낼것인가 (두자리면 2, 세자리면 3)
    def roundingDouble(colValD:Double) = {
      val roundingPoint = 2.0

      if (colValD == 0.0) {
        colValD
      } else {
        val roundPt =  Math.pow(10, roundingPoint)
        val v = Math.round(colValD * roundPt)
        v.toDouble / roundPt
      }
    }

    //Number 형태 스트링 자르기
    def trimDouble(ret: String, number: String): String = {
      if (number.length > 0) {
        if (number(0) == '.') {
          ret + "." + (number + "00").substring(1, 3)
        } else {
          trimDouble(ret + number(0), number.substring(1))
        }
      } else {
        ret
      }
    }

    //반올림
    def round(colVal: String) = {
      val colValD = colVal.toDouble
      val res = roundingDouble(colValD)
      res
    }

    val p = raw"^-?\d+[.]?\d+".r //232323.232322 , 2323232 이런경우
    val e = raw"^-?\d+[.]\d+E[-\d]+".r //334343.3434E33 이런경우

    if (colVal != null) {
      colVal match {
        case p() => {
//          println("==>"+colVal)
          val v = round(colVal)
          val c = trimDouble("", v.toString)
          Some(c)
        }
        case e() => {
//          println("+++>"+colVal)
          val v = round(colVal)
          val c = trimDouble("", v.toString)
          Some(c)
        }
        case _ => Some(colVal)
      }
    } else None
  }

  def roundAndCastOldVer(beerDf: DataFrame, whiskyDf: DataFrame) = {
    val trimUdf = udf((colVal:String) => trimD(colVal))

    val exceptList = List("NaN", "Infinity", "-")

    // 반올림하는 컬럼들 및 반올림 안하더라도 컬럼 순서상 포함되어 있는 컬럼들
    val rootCols = Seq("MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDR3", "YM").map(col)

    val qtCols = Seq(
      trimUdf('QT_MARKET).as("QT_MARKET"),
      trimUdf('QT_MARKET_DK).as("QT_MARKET_DK"),
      when('QT_SOM.isin(exceptList:_*), 'QT_SOM).otherwise(trimUdf('QT_SOM)).as("QT_SOM"),
      when('QT_GAP.isin(exceptList:_*), 'QT_GAP).otherwise(trimUdf('QT_GAP)).as("QT_GAP")
    )

    val amtCols = Seq(
      trimUdf('AMT_MARKET).as("AMT_MARKET"),
      trimUdf('AMT_MARKET_DK).as("AMT_MARKET_DK"),
      when('AMT_SOM.isin(exceptList:_*), 'AMT_SOM).otherwise(trimUdf('AMT_SOM)).as("AMT_SOM"),
      when('AMT_GAP.isin(exceptList:_*), 'AMT_GAP).otherwise(trimUdf('AMT_GAP)).as("AMT_GAP")
    )

    val btlCols = Seq(
      trimUdf('BOTTLE_MARKET).as("BOTTLE_MARKET"),
      trimUdf('BOTTLE_MARKET_DK).as("BOTTLE_MARKET_DK"),
      when('BOTTLE_SOM.isin(exceptList:_*), 'BOTTLE_SOM).otherwise(trimUdf('BOTTLE_SOM)).as("BOTTLE_SOM"),
      when('BOTTLE_GAP.isin(exceptList:_*), 'BOTTLE_GAP).otherwise(trimUdf('BOTTLE_GAP)).as("BOTTLE_GAP")
    )

    val dealCols = Seq(
      trimUdf('DEAL_MARKET).as("DEAL_MARKET"),
      trimUdf('DEAL_MARKET_DK).as("DEAL_MARKET_DK"),
      when('DEAL_SOM.isin(exceptList:_*), 'DEAL_SOM).otherwise(trimUdf('DEAL_SOM)).as("DEAL_SOM"),
      when('DEAL_GAP.isin(exceptList:_*), 'DEAL_GAP).otherwise(trimUdf('DEAL_GAP)).as("DEAL_GAP")
    )

    val slctCols = rootCols ++ qtCols ++ amtCols ++ btlCols ++ dealCols

    val beerRoundDf = beerDf.select(slctCols:_*)
    val whiskyRoundDf = whiskyDf.select(slctCols:_*)
    (beerRoundDf, whiskyRoundDf)
  }

  // 딜러수 3 미만 마스킹 처리
  def masking(beerDf: DataFrame, whiskyDf: DataFrame) = {
    // 마스킹 처리하지 않는 기본 컬럼들
    val rootCols = Seq("MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDR3", "YM").map(col)

    // 마스킹 처리해야 하는 컬럼들
    val maskingCols = Seq(
      lit("-").as("QT_MARKET"), lit("-").as("QT_MARKET_DK"),
      lit("-").as("QT_SOM"), lit("-").as("QT_GAP"),
      lit("-").as("AMT_MARKET"), lit("-").as("AMT_MARKET_DK"),
      lit("-").as("AMT_SOM"), lit("-").as("AMT_GAP"),
      lit("-").as("BOTTLE_MARKET"), lit("-").as("BOTTLE_MARKET_DK"),
      lit("-").as("BOTTLE_SOM"), lit("-").as("BOTTLE_GAP"),
      lit("-").as("DEAL_MARKET"), lit("-").as("DEAL_MARKET_DK"),
      lit("-").as("DEAL_SOM"), lit("-").as("DEAL_GAP")
    )

    // 최종 select 컬럼들
    val slctCols = rootCols ++ maskingCols

    // DEAL_MARKET(딜러수) 3 미만인 데이터
    val bDmLessThan3Df = beerDf.filter('DEAL_MARKET < 3)
    val wDmLessThan3Df = whiskyDf.filter('DEAL_MARKET < 3)

    // DEAL_MARKET(딜러수) 3 미만인 데이터 마스킹 처리
    val bDmLessThan3MaskingDf = bDmLessThan3Df.select(slctCols:_*)
    val wDmLessThan3MaskingDf = wDmLessThan3Df.select(slctCols:_*)

    // DEAL_MARKET(딜러수) 3 이상인 데이터 마스킹 미처리
    val bDmMoreThan3Df = beerDf.filter('DEAL_MARKET >= 3 || 'YM.isNull)   // YM 이 null 인 데이터는 과거 히스토리 데이터
    val wDmMoreThan3Df = whiskyDf.filter('DEAL_MARKET >= 3 || 'YM.isNull) // YM 이 null 인 데이터는 과거 히스토리 데이터

    // 마스킹 미처리 데이터 + 마스킹 처리 데이터
    val beerMaskingDf = bDmMoreThan3Df.union(bDmLessThan3MaskingDf)
    val whiskyMaskingDf = wDmMoreThan3Df.union(wDmLessThan3MaskingDf)
    (beerMaskingDf, whiskyMaskingDf)
  }

  // DK 회계 연월 변환
  def chngDkYm(beerDf: DataFrame, whiskyDf: DataFrame)= {
    // ex: 2017-07 =>  F1801
    val chngDkYmUdf = udf((ym: String) => {
      if (ym == null) {
        null
      } else {
        val (dkYm, _, _) = dkYmCls(ym)
        dkYm
      }
    })

    val beerDkYmDf = beerDf.withColumn("YM", chngDkYmUdf('YM))
    val whiskyDkYmDf = whiskyDf.withColumn("YM", chngDkYmUdf('YM))
    (beerDkYmDf, whiskyDkYmDf)
  }

  // 해당 집계 연월 결과 데이터프레임 + 과거 히스토리 데이터프레임
  def resDkUnionHstryDk(resDk: DataFrame, hstryDk: DataFrame) = {
    val totalDk = resDk.union(hstryDk)
    totalDk
  }

  // 해당 집계 연월 결과 데이터프레임 + 과거 히스토리 데이터프레임
  def resDkUnionHstryDkNew(ethDt:String, resDk: DataFrame, hstryDk: DataFrame) = {
    val dkCol = Seq( "YM", "MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDRESS",
      "QT_MARKET", "QT_SOM", "QT_GAP", "AMT_MARKET", "AMT_SOM", "AMT_GAP",
      "BOTTLE_MARKET", "BOTTLE_SOM", "BOTTLE_GAP", "DEALER_MARKET", "DEALER_SOM", "DEALER_GAP").map(col)
    val totalDk = resDk.select(dkCol: _*)
    totalDk
  }


  // 메인
  def getTotalDk(ethDt: String,
                 resDkb: DataFrame,
                 resDkw: DataFrame,
                 hstryDkb: DataFrame,
                 hstryDkw: DataFrame) = {
    // QT 를 EU 로 단위 환산
    val (resDkbEu, resDkwEu) = trnsfQtToEu(resDkb, resDkw)
    val (hstryDkbEu, hstryDkwEu) = trnsfQtToEu(hstryDkb, hstryDkw)

    // AMT 를 백 만 단위로 환산
    val (resDkbUnit, resDkwUnit) = trnsfAmtToUnit(resDkbEu, resDkwEu)
    val (hstryDkbUnit, hstryDkwUnit) = trnsfAmtToUnit(hstryDkbEu, hstryDkwEu)

    // 소수 셋째 짜리에서 반올림 및 소수 둘째 짜리까지 추출
    val (resDkbRound, resDkwRound) = roundAndCastOldVer(resDkbUnit, resDkwUnit)
    val (hstryDkbRound, hstryDkwRound) = roundAndCastOldVer(hstryDkbUnit, hstryDkwUnit)

    // 마스킹 처리
    val (resDkbMasking, resDkwMasking) = masking(resDkbRound, resDkwRound)
    val (hstryDkbMasking, hstryDkwMasking) = masking(hstryDkbRound, hstryDkwRound)

    // DK 회계 연월 변환
    val (resDkbDkYm, resDkwDkYm) = chngDkYm(resDkbMasking, resDkwMasking)
    val (hstryDkbDkYm, hstryDkwDkYm) = chngDkYm(hstryDkbMasking, hstryDkwMasking)

    // beer, whisky 별로 res 랑 hstry 합치기
    val totalDkb = resDkUnionHstryDk(resDkbDkYm, hstryDkbDkYm)
    val totalDkw = resDkUnionHstryDk(resDkwDkYm, hstryDkwDkYm)
    (totalDkb, totalDkw)
  }
  // 메인
  def getTotalDkNew(ethDt: String,
                 resDkb: DataFrame,
                 resDkw: DataFrame) = {
    // QT 를 EU 로 단위 환산
    val (resDkbEu, resDkwEu) = trnsfQtToEu(resDkb, resDkw)

    // AMT 를 백 만 단위로 환산
    val (resDkbUnit, resDkwUnit) = trnsfAmtToUnit(resDkbEu, resDkwEu)

    // 소수 셋째 짜리에서 반올림 및 소수 둘째 짜리까지 추출
    val (resDkbRound, resDkwRound) = roundAndCastOldVer(resDkbUnit, resDkwUnit)

    // 마스킹 처리
    val (resDkbMasking, resDkwMasking) = masking(resDkbRound, resDkwRound)

    // DK 회계 연월 변환
    val (resDkbDkYmTmp, resDkwDkYmTmp) = chngDkYm(resDkbMasking, resDkwMasking)

    // 주소 ADDR3 -> ADDRESS
    val (resDkbDkYm, resDkwDkYm) = getViewResCls.getViewResNew(ethDt, resDkbDkYmTmp, resDkwDkYmTmp)
    (resDkbDkYm, resDkwDkYm)
  }
}