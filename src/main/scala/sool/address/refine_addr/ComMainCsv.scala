/**

 */

package sool.address.refine_addr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat_ws, lit, rank}

class ComMainCsv(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  /*
  com_main.csv 생성
  코드 수정 시, 변수명이 대부분 문자 + 숫자로 되어 있기 때문에 조심할 것!
  현재 상태는 흐름이 중요하기 때문에 따로 함수로 안빼고 아래 함수 안에서 다 처리되도록 함.
   */
  def apply(comRgnoRprsnBmnCnLen: DataFrame, bldngInfoView: DataFrame) = {
    /*
    건물정보 테이블 수정
    SIGUNGU_NM 이 null 인 경우가 있다.(ex: 세종특별자치시) 반드시 null 을 "" 으로 바꿔줘야 한다.
     */
    val bldngInfoNaFill = bldngInfoView.na.fill("", Seq("SIGUNGU_NM"))
    val bldngInfo = bldngInfoNaFill.select('BLDNG_MGMT_NMB, 'PNU, concat_ws(",", 'SIDO_NM, 'SIGUNGU_NM,
      'LEGAL_EPMYNDNG_NM, 'JIBEON, 'ROAD_NM, 'BLDNG_BON_BUBEON, 'POST_CD).as("COM_ADDR"))

    /* 건물정보 DB VIEW 에 건물관리번호가 있는 경우 */
    val comMainCsv1Pre1 = comRgnoRprsnBmnCnLen.join(bldngInfo, Seq("BLDNG_MGMT_NMB"), "left_outer")
    val comMainCsv1Pre2 = comMainCsv1Pre1.filter('COM_ADDR.isNotNull)
    val comMainCsv1 = comMainCsv1Pre2.select('COM_RGNO, 'COM_NM, 'COM_ADDR, 'LEN, lit("TYPE1").as("TYPE"))

    /* 건물정보 DB VIEW 에 건물관리번호가 없는 경우 PNU 로 매칭 */
    val comMainCsv2Pre1 = comMainCsv1Pre1.filter('COM_ADDR.isNull).drop("PNU", "COM_ADDR")
    val comMainCsv2Pre2 = comMainCsv2Pre1.withColumn("PNU", 'BLDNG_MGMT_NMB.substr(0, 19)).
      drop("BLDNG_MGMT_NMB")
    val comMainCsv2Pre3 = comMainCsv2Pre2.join(bldngInfo, Seq("PNU"), "left_outer")   // PNU 추가

    // 아무 거나 한 개 선택하면 돼서 빌딩관리번호, 주소 오름차순 순으로 1개만 추출한다.
    val w = Window.partitionBy("COM_RGNO", "COM_NM", "PNU", "LEN").orderBy($"BLDNG_MGMT_NMB", $"COM_ADDR")
    val comMainCsv2Pre4 = comMainCsv2Pre3.withColumn("RANK", rank.over(w)).filter('RANK === 1).
      drop("RANK")
    val comMainCsv2Pre5 = comMainCsv2Pre4.filter('COM_ADDR.isNotNull)
    val comMainCsv2 = comMainCsv2Pre5.select('COM_RGNO, 'COM_NM, 'COM_ADDR, 'LEN, lit("TYPE1").as("TYPE"))

    // 최종 결과
    val comMainCsv = comMainCsv1.union(comMainCsv2)
    comMainCsv
  }
}
