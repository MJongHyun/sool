/**

 */

package sool.address.refine_addr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, rank, udf}
import org.apache.spark.sql.expressions.Window

class AddrMatchingDivOrdr(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 모집단 데이터에 건물관리번호 매칭
  def getAddrMatching(moAddrIdx: DataFrame, dtiAddr: DataFrame) = {
    /* moAddr 에서 중복이 존재할 수 있다.
    같은 사업자, 사업자, 판매량 혹은 매입량인데 원천 주소가 다르고 각각의 원천 주소가 같은 건물관리번호인 경우
    moAddr 와 dtiAddr 랑 매칭했을 때 중복 데이터가 존재할 수 있다. 마지막 결과물에 distinct 로 중복을 제거한다.
     */
    val moAddrTrimUdf = udf((comAddr: String) => if (comAddr == null || comAddr == "" || comAddr == " ") comAddr else comAddr.trim())
    val moAddrTrim = moAddrIdx.withColumn("COM_ADDR", moAddrTrimUdf('COM_ADDR))  // moAddr 원천 주소 trim 처리
    val moAddrRnmCl = moAddrTrim.withColumnRenamed("COM_ADDR", "SRC_ADRS")

    // 모집단 데이터의 원천주소 값과 DB 에 저장되어 있는 원천주소별 건물관리번호 매칭
    val dtiAddrSlct = dtiAddr.select("BLDNG_MGMT_NMB", "SRC_ADRS")
    val moAddrMatchingPre1 = moAddrRnmCl.join(dtiAddrSlct, Seq("SRC_ADRS"), "left_outer")
    val moAddrMatchingPre2 = moAddrMatchingPre1.na.fill("", Seq("BLDNG_MGMT_NMB"))

    // 모집단 데이터의 인덱스를 기준으로 동일 건물관리번호 중복값 제거 목적
    val moAddrMatchingPre2Cols = Seq("COM_RGNO", "COM_NM", "DIV", "DIV_AMT", "BLDNG_MGMT_NMB").map(col)
    val w = Window.partitionBy(moAddrMatchingPre2Cols:_*).orderBy("IDX")
    val addrMatching = moAddrMatchingPre2.
      withColumn("RANK", rank.over(w)).
      filter('RANK === 1).
      drop("RANK", "SRC_ADRS")
    addrMatching
  }



  // 모집단별 순서 테이블
  def getDivOrdrTbl() = Seq(("HJ모집단맥주", 1), ("HJ모집단소주", 2), ("그외값", 3)).toDF("DIV", "DIV_ORDER")

  // 모집단 데이터에 건물관리번호 매칭 및 모집단별 순서 컬럼 추가
  def runAddrMatchingDivOrdr(moAddrIdx: DataFrame, dtiAddr: DataFrame) = {
    val addrMatching = getAddrMatching(moAddrIdx, dtiAddr)   // 모집단 데이터에 건물관리번호 매칭
    val divOrdrTbl = getDivOrdrTbl()  // 모집단별 순서 테이블
    val addrMatchingDivOrdr = addrMatching.join(divOrdrTbl, Seq("DIV"))
    addrMatchingDivOrdr
  }
}
