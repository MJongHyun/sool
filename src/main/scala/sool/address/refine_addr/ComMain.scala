package sool.address.refine_addr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat_ws, lit, rank, size, split, sum, translate, when, length}

class ComMain(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  def getDfs(ethDt: String, comMainCsvLoad: DataFrame, fixAddrLoad:DataFrame) = {
    /* 정제주소 결과데이터
    사업자, 사업자명, 정제주소결과, 정제주소개수,
    구분 (1: 이번에 정제된 주소, 3:정제는 되었지만 주소 이슈로 전 달 주소 가져온 것 -> 이 중 주소 없는 것 제외)
     */
    val comMainCsv = comMainCsvLoad.
      toDF("COM_RGNO", "COM_NM", "ADDR", "LEN", "TYPE").
      filter('COM_NM.isNotNull)

    // 고정주소에 해당하는 값 제외하고 정제주소 값들만 추출
    val fixAddr = fixAddrLoad.
      withColumn("ADDR_DATE",lit(ethDt))
    val fixRgno = fixAddr.select('COM_RGNO)

    (comMainCsv, fixAddr, fixRgno)
  }

  // 주류 1차 분석데이터에서 사업자, 상호명 별 공급/매입금액 추출
  def getTrgtCom(base: DataFrame) = {
    // 주류 1차 분석데이터에서 사업자, 상호명 별 공급/매입금액 추출
    val baseSup = base.select('SUP_RGNO.as("COM_RGNO"), 'SUP_NM.as("COM_NM"), 'SUP_AMT)
    val baseByr = base.select('BYR_RGNO.as("COM_RGNO"), 'BYR_NM.as("COM_NM"), 'SUP_AMT)
    val baseCom = baseSup.union(baseByr)
    val trgtCom = baseCom.groupBy($"COM_RGNO", $"COM_NM").agg(sum($"SUP_AMT").as("AMT"))
    val trgtComRgno = trgtCom.select('COM_RGNO).distinct()

    /* 사업자, 상호명 별 금액 데이터에서, 금액이 가장 높은 값에 해당하는 상호명을 대표 상호명으로 설정
    금액이 같을 경우, 상호명 오름차순으로 상호명 설정 */
    val w = Window.partitionBy("COM_RGNO").orderBy($"AMT".desc, $"COM_NM")
    val trgtComRnkFltr = trgtCom.
      withColumn("RANK", rank().over(w)).
      filter('RANK === 1).
      select('COM_RGNO, 'COM_NM)

    (trgtComRgno, trgtComRnkFltr)
  }

  // 신규 주류 프로세스를 통해 얻은 결과와 base 에서 추출한 사업자와 조인
  def getComMain1(ethDt: String,
                  trgtComRgno: DataFrame,
                  comMainCsv: DataFrame,
                  fixRgno: DataFrame) = {
    // 고정 주소 제외
    val comMainCsvExceptFixRgno = comMainCsv.join(fixRgno, Seq("COM_RGNO"), "left_anti")

    // 정제된 결과들을 업체 마스터 데이터 형식에 맞도록 정제 후 추출
    val comMainTrgtPre = comMainCsvExceptFixRgno.join(trgtComRgno, Seq("COM_RGNO"))
    val comMainTrgt = comMainTrgtPre.
      withColumn("ADDR", translate($"ADDR", " ", ",")).
      withColumn("ADDR_SPLIT", split($"ADDR", ",")).
      withColumn("ADDR_LEN", size($"ADDR_SPLIT")).
      withColumn("ADDR_TYPE", lit("NEW")).
      withColumn("ADDR_DATE", lit(ethDt))

    // 주소 길이가 7인 경우 : 시도 + 시군구 + 읍면동 + 지번 + 도로명 + 도로명번호 + 우편번호
    val comMain1Case1 = comMainTrgt.
      filter($"ADDR_LEN" === 7).
      withColumn("ADDR1", $"ADDR_SPLIT".getItem(0)).
      withColumn("ADDR2", $"ADDR_SPLIT".getItem(1)).
      withColumn("ADDR3D", $"ADDR_SPLIT".getItem(2)).
      withColumn("DNUM", $"ADDR_SPLIT".getItem(3)).
      withColumn("ADDR3R", $"ADDR_SPLIT".getItem(4)).
      withColumn("RNUM", $"ADDR_SPLIT".getItem(5)).
      withColumn("POST_CD", $"ADDR_SPLIT".getItem(6)).
      withColumn("ADDR3", concat_ws(" ", $"ADDR3D", $"ADDR3R")).
      select('COM_RGNO, 'COM_NM, 'ADDR1, 'ADDR2, 'ADDR3D, 'ADDR3R, 'ADDR3,
        'DNUM, 'RNUM, 'POST_CD, 'LEN, 'ADDR_TYPE, 'ADDR_DATE).toDF()

    // 주소 길이가 8인 경우 : 시도 + 시 + 시군구 + 읍면동 + 지번 + 도로명 + 도로명번호 + 우편번호
    val comMain1Case2 = comMainTrgt.
      filter($"ADDR_LEN" === 8).
      withColumn("ADDR1", $"ADDR_SPLIT".getItem(0)).
      withColumn("ADDR2", concat_ws(" ", $"ADDR_SPLIT".getItem(1), $"ADDR_SPLIT".getItem(2))).
      withColumn("ADDR3D", $"ADDR_SPLIT".getItem(3)).
      withColumn("DNUM", $"ADDR_SPLIT".getItem(4)).
      withColumn("ADDR3R", $"ADDR_SPLIT".getItem(5)).
      withColumn("RNUM", $"ADDR_SPLIT".getItem(6)).
      withColumn("POST_CD", $"ADDR_SPLIT".getItem(7)).
      withColumn("ADDR3", concat_ws(" ", $"ADDR3D", $"ADDR3R")).
      select('COM_RGNO, 'COM_NM, 'ADDR1, 'ADDR2, 'ADDR3D, 'ADDR3R, 'ADDR3,
        'DNUM, 'RNUM, 'POST_CD, 'LEN, 'ADDR_TYPE, 'ADDR_DATE).toDF()

    val comMain1 = comMain1Case1.union(comMain1Case2)
    comMain1
  }

  /*
   신규 주류 프로세스로 부터 사업자명, 주소를 구한 사업자를 제외한 나머지 사업자들은 금액순, 상호명 오름차순 순으로 순위를 매긴 후 1등 사업자명을 추출한다.
   각 사업자의 주소는 저번 달 업체 마스터로부터 구하고, 위에서 추출한 사업자명을 붙인다.
   */
  def getComMain2(trgtComRnkFltr: DataFrame, comMain1: DataFrame, fixRgno: DataFrame, comMainBf1m: DataFrame) = {
    // 저번달 comMain 을 붙인다.
    // base 로부터 금액 내림차순, 업체명 오름차순으로 추출한 trgtComRnkFltr 중에 이미 주소 붙은 comMain1 은 제외한 값을 구한다.
    val comMain2TrgtRgno1 = trgtComRnkFltr.select('COM_RGNO).except(comMain1.select('COM_RGNO))
    val comMain2TrgtRgno2 = comMain2TrgtRgno1.join(fixRgno, Seq("COM_RGNO"), "left_anti") // 고정 주소 제외
    val comMain2TrgtRgnoNm = comMain2TrgtRgno2.join(trgtComRnkFltr, Seq("COM_RGNO"),"left")

    // base 로부터 금액 내림차순, 업체명 오름차순으로 추출한 업체명을 쓰기 위해 저번 달 com_main 에서의 업체명 컬럼은 버린다.
    val comMainBf1mFltr = comMainBf1m.filter('COM_NM.isNotNull).drop("COM_NM")
    val comMain2 = comMain2TrgtRgnoNm.join(comMainBf1mFltr, Seq("COM_RGNO"))  // 여기서 ADDR_TYPE 컬럼에 BF1M, OLD 가 추가됨
    comMain2
  }

  def getComMain3(ethDt: String,
                  trgtComRnkFltr: DataFrame,
                  comMain1and2: DataFrame,
                  fixRgno: DataFrame,
                  comMainPre: DataFrame) = {
    // 이번 달 과거 로직 정제 주소 결과 데이터에 대표상호명을 적용하여 추출 (이번 달 과거 로직 정제 주소 결과인 경우는 정제주소 갯수 값에 0 을 기입)
    val comMain3TrgtRgno1 = trgtComRnkFltr.select('COM_RGNO).except(comMain1and2.select('COM_RGNO))
    val comMain3TrgtRgno = comMain3TrgtRgno1.join(fixRgno, Seq("COM_RGNO"), "left_anti") // 고정 주소 제외
    val comMain3TrgtRgnoNm = comMain3TrgtRgno.join(trgtComRnkFltr, Seq("COM_RGNO"),"left")
    val comMainPreSlct = comMainPre.select('COM_RGNO, 'ADDR1, 'ADDR2, 'ADDR3D, 'ADDR3R, 'ADDR3, 'DNUM, 'RNUM, 'POST_CD)
    val comMain3TrnWithCmp = comMain3TrgtRgnoNm.join(comMainPreSlct, Seq("COM_RGNO"), "left")
    val comMain3 = comMain3TrnWithCmp.
      withColumn("COM_NM", when($"ADDR1".isNull, lit(null)).otherwise($"COM_NM")).
      withColumn("LEN", when($"ADDR1".isNull, lit(null)).otherwise(lit("0"))).
      withColumn("ADDR_TYPE", when($"ADDR1".isNull, lit(null)).otherwise(lit("OLD"))).
      withColumn("ADDR_DATE", when($"ADDR1".isNull, lit(null)).otherwise(lit(ethDt))).
      withColumn("ADDR3D_SPLIT", split($"ADDR3D", " ")).
      withColumn("ADDR3D_LEN", size($"ADDR3D_SPLIT")).
      withColumn("GU", when($"ADDR3D_LEN" === 2, $"ADDR3D_SPLIT".getItem(0)).otherwise("")).
      withColumn("GU_EXSIT", length($"GU")).
      withColumn("ADDR2", when($"GU_EXSIT" === 0, $"ADDR2").otherwise(concat_ws(" ", $"ADDR2", $"GU"))).
      withColumn("ADDR3D", when($"ADDR3D_LEN" === 2, $"ADDR3D_SPLIT".getItem(1)).otherwise($"ADDR3D_SPLIT".getItem(0))).
      withColumn("ADDR3", when($"ADDR3D".isNull, lit(null)).otherwise(concat_ws(" ", $"ADDR3D", $"ADDR3R"))).
      drop("ADDR3D_SPLIT", "ADDR3D_LEN", "GU", "GU_EXSIT")
    comMain3
  }

  // DK 결과를 위한 업체 마스터 생성
  def getComMainDk(comMain: DataFrame) = {
    val comMainDk = comMain.
      withColumn("ADDR2_SPLIT", split($"ADDR2", " ")).
      withColumn("SPLIT_LEN", size($"ADDR2_SPLIT")).
      withColumn("GU", when($"SPLIT_LEN" === 2, $"ADDR2_SPLIT".getItem(1)).otherwise("")).
      withColumn("GU_EXSIT", length($"GU")).
      withColumn("ADDR2", when($"GU_EXSIT" > 0, $"ADDR2_SPLIT".getItem(0)).otherwise($"ADDR2")).
      withColumn("ADDR3D", when($"GU_EXSIT" > 0, concat_ws(" ", $"GU", $"ADDR3D")).otherwise($"ADDR3D")).
      withColumn("ADDR3", when($"ADDR3D".isNull, lit(null)).otherwise(concat_ws(" ", $"ADDR3D", $"ADDR3R"))).
      drop("ADDR2_SPLIT", "SPLIT_LEN", "GU", "GU_EXSIT").toDF()
    comMainDk
  }

  // com_main.parquet 체크
  def checkComMain(comMain: DataFrame, ethDt: String) = {
    println("----------------------------------------")
    println("전체 업체 수 : " + comMain.count())
    println("----------------------------------------")
    println("신규 프로세스로 정제된 업체 수 : " + comMain.filter($"ADDR_TYPE" === "NEW").count())
    println("이번 분석연월에 신규 프로세스로 정제된 업체 수 : " + comMain.filter($"ADDR_TYPE" === "NEW").filter($"ADDR_DATE" === ethDt).count())
    println("지난 분석연월에 신규 프로세스로 정제된 업체 수 : " + comMain.filter($"ADDR_TYPE" === "NEW").filter($"ADDR_DATE" =!= ethDt).count())
    println("----------------------------------------")
    println("과거 프로세스로 정제된 업체 수 : " + comMain.filter($"ADDR_TYPE" === "OLD").count())
    println("이번 분석연월에 과거 프로세스로 정제된 업체 수 : " + comMain.filter($"ADDR_TYPE" === "OLD").filter($"ADDR_DATE" === ethDt).count())
    println("지난 분석연월에 과거 프로세스로 정제된 업체 수 : " + comMain.filter($"ADDR_TYPE" === "OLD").filter($"ADDR_DATE" =!= ethDt).count())
    println("----------------------------------------")
    println("출처를 알 수 없는 과거에 정제된 업체 수 : " + comMain.filter($"ADDR_TYPE" === "BF1M").count())
    println("정제되지 않은 업체 수 : " + comMain.filter($"ADDR_TYPE".isNull).count())
    println("----------------------------------------")
  }

  // 메인
  def getComMain(ethDt: String,
                 base: DataFrame,
                 comMainCsvLoad: DataFrame,
                 fixAddrLoad: DataFrame,
                 comMainBf1m: DataFrame,
                 comMainPre: DataFrame) = {
    // 필요 데이터
    val (comMainCsv, fixAddr, fixRgno) = getDfs(ethDt, comMainCsvLoad, fixAddrLoad)

    // 주류 1차 분석데이터에서 사업자, 상호명 별 공급/매입금액 추출
    val (trgtComRgno, trgtComRnkFltr) = getTrgtCom(base)

    // 신규 주류 프로세스를 통해 얻은 결과와 base 에서 추출한 사업자와 조인
    val comMain1 = getComMain1(ethDt, trgtComRgno, comMainCsv, fixRgno)
    val comMain2 = getComMain2(trgtComRnkFltr, comMain1, fixRgno, comMainBf1m)
    val comMain1and2 = comMain1.union(comMain2)
    val comMain3 = getComMain3(ethDt, trgtComRnkFltr, comMain1and2, fixRgno, comMainPre)
    val comMain = comMain1and2.union(comMain3).union(fixAddr)
    val comMainDk = getComMainDk(comMain)
    (comMain, comMainDk)
  }
}
