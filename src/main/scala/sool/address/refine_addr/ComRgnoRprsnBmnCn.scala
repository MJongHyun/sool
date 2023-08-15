/**

 */

package sool.address.refine_addr

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.DataFrame

class ComRgnoRprsnBmnCn(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 모집단별 순서, 매입량 높은 순, 인덱스 낮은 순으로 순위를 매겨 대표 1개만 추출
  def getRankDf(df: DataFrame) = {
    // 로직이 변경되는 거 아닌 이상 orderBy 순서 변경하지 말 것
    val w = Window.partitionBy("COM_RGNO").orderBy($"DIV_ORDER", $"DIV_AMT".desc, $"IDX")
    val res = df.
      withColumn("DIV_AMT", 'DIV_AMT.cast("Double")).
      withColumn("RANK", rank.over(w)).
      filter('RANK === 1).
      drop("DIV_ORDER", "RANK", "IDX")
    res
  }

  // 사업자별 대표 건물관리번호, 사업자명 매칭
  def getComRgnoRprsnBmnCn(addrMatchingDivOrdr: DataFrame, rprsnBldngMgmtNmb: DataFrame, rprsnComNm:DataFrame) = {
    val addrMatchingComRgno = addrMatchingDivOrdr.select("COM_RGNO").distinct
    val comRgnoWithBldngMgmtNmb = addrMatchingComRgno.join(rprsnBldngMgmtNmb, Seq("COM_RGNO"), "left_outer")
    val comRgnoWithComRgno = comRgnoWithBldngMgmtNmb.join(rprsnComNm, Seq("COM_RGNO"), "left_outer")
    val comRgnoRprsnBmnCn = comRgnoWithComRgno.filter('BLDNG_MGMT_NMB.isNotNull && 'COM_RGNO.isNotNull)
    comRgnoRprsnBmnCn
  }

  // 사업자별 건물관리번호 개수인 LEN 컬럼 추가 목적 데이터프레임
  def getAddLenCol(addrMatchingDivOrdr: DataFrame) = {
    val addLenColPre = addrMatchingDivOrdr.
      filter('BLDNG_MGMT_NMB.isNotNull && 'BLDNG_MGMT_NMB =!= "").
      select("COM_RGNO", "BLDNG_MGMT_NMB").
      distinct()
    val addLenCol = addLenColPre.groupBy("COM_RGNO").count().withColumnRenamed("count", "LEN")
    addLenCol
  }

  // 사업자별 대표 건물관리번호, 사업자명, LEN 데이터프레임 생성
  def runComRgnoRprsnBmnCnLen(addrMatchingDivOrdr: DataFrame) = {
    // 사업자별 대표 건물관리번호
    val rprsnBldngMgmtNmbPre = addrMatchingDivOrdr.filter('BLDNG_MGMT_NMB.isNotNull && 'BLDNG_MGMT_NMB =!= "").drop("COM_NM")
    val rprsnBldngMgmtNmb = getRankDf(rprsnBldngMgmtNmbPre).select("COM_RGNO", "BLDNG_MGMT_NMB")

    // 사업자별 대표 사업자명
    val rprsnComNmPre = addrMatchingDivOrdr.filter('COM_NM.isNotNull && 'COM_NM =!= "").drop("BLDNG_MGMT_NMB")
    val rprsnComNm = getRankDf(rprsnComNmPre).select("COM_RGNO", "COM_NM")

    // 사업자별 대표 건물관리번호, 사업자명 매칭
    val comRgnoRprsnBmnCn = getComRgnoRprsnBmnCn(addrMatchingDivOrdr, rprsnBldngMgmtNmb, rprsnComNm)

    // 사업자별 대표 건물관리번호, 사업자명, LEN 데이터프레임 생성
    val addLenCol = getAddLenCol(addrMatchingDivOrdr)
    val comRgnoRprsnBmnCnLen = comRgnoRprsnBmnCn.join(addLenCol, Seq("COM_RGNO"))
    comRgnoRprsnBmnCnLen
  }
}
