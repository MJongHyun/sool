/**
 * 업체가 매입한 아이템을 기준으로 모집단을 만들어 사업자, 상호명, 주소, 모집단에 따르는 매입금액 추출
 * todo: HJ 것만 사용하여 moAddr 를 구했었는데 이러면 DK, HK만 거래하는 애들 같은 경우 버려지기 때문에 수정이 필요함

 */
package sool.address.refine_addr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, sum, when}
import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath

class MoAddr(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)

  // HJ모집단에 해당하는 아이템 추출
  def getHjItem() = {
    val hjbItem = jdbcGetCls.getItemTbl("HJ_BR_ITEM")
    val hjsItem = jdbcGetCls.getItemTbl("HJ_SJ_ITEM")
    val hjbItemSlct = hjbItem.select('BR_NM.as("ITEM"), lit("HJ모집단맥주").as("HJ_MO"))
    val hjsItemSlct = hjsItem.select('SJ_NM.as("ITEM"), lit("HJ모집단소주").as("HJ_MO"))
    val hjItem = hjbItemSlct.union(hjsItemSlct)
    hjItem
  }

  // 누적사전에서 HJ 모집단 아이템에 해당하는 것만 추출
  def getCumulHj(cumulNminfoDf: DataFrame, hjItem: DataFrame) = {
    val cumulHj = cumulNminfoDf.
      join(hjItem, Seq("ITEM")).
      filter('SAW =!= "안봄").  // 명확히 검토한 품목만 선정안
      select('NAME, 'ITEM_SZ, 'ITEM, 'HJ_MO)
    cumulHj
  }

  // 아이템을 부착하기 위해, 아이템명과 사이즈를 정제 후, HJ 모집단 아이템 기반 누적사전과 JOIN하여 데이터 추출
  def getBaseWithCumulHj(base: DataFrame, cumulHj: DataFrame) = {
    val baseSlct = base.select(
      'SUP_RGNO, 'SUP_NM, 'SUP_ADDR, 'BYR_RGNO, 'BYR_NM,
      'BYR_ADDR, 'ITEM_NM, 'ITEM_SZ, 'NAME, 'SUP_AMT
    )
    val baseSlctFltr = baseSlct.filter('ITEM_SZ.isNotNull)
    val baseJoinCumulHj = baseSlctFltr.join(cumulHj, Seq("NAME", "ITEM_SZ"), "left_outer")

    // HJ모집단맥주, HJ모집단소주에 해당하지 않을 경우, 그외값으로 표기
    val baseWithCumulHj = baseJoinCumulHj.
      withColumn("DIV", when($"HJ_MO".isNull, lit("그외값")).otherwise($"HJ_MO"))
    baseWithCumulHj
  }

  // 업체별 상호명, 주소, 모집단에 따른 아이템 매입금액 추출
  def getMoAmt(baseWithCumulHj: DataFrame) = {
    val moSupAmt = baseWithCumulHj.
      groupBy("SUP_RGNO", "SUP_NM", "SUP_ADDR", "DIV").agg(sum("SUP_AMT").as("MO_AMT"))
    val moByrAmt = baseWithCumulHj.
      groupBy("BYR_RGNO", "BYR_NM", "BYR_ADDR", "DIV").agg(sum("SUP_AMT").as("MO_AMT"))
    val moAmt = moSupAmt.union(moByrAmt).toDF("COM_RGNO", "COM_NM", "COM_ADDR", "DIV", "DIV_AMT")
    moAmt
  }

  // 메인
  def runMoAddr(ethDtBf1m: String, base: DataFrame, flag: String) = {
    val filePathClsBf1m = new FilePath(ethDtBf1m, flag)

    // todo: HJ 것만 사용하여 moAddr 를 구했었는데 이러면 DK, HK만 거래하는 애들 같은 경우 버려지기 때문에 수정이 필요함
    // HJ모집단에 해당하는 아이템 추출 ---> HJ 를 사용하는 이유(히스토리)가 있음
    val hjItem = getHjItem()

    // 누적 아이템 태깅 결과를 통해 아이템에 따르는 정제된 아이템명과 아이템 사이즈 추출
    val cumulNminfoDf = if (fileFuncCls.checkS3FileExist(filePathClsBf1m.cumulNminfoDfNewPath)) {
      fileFuncCls.rParquet(filePathClsBf1m.cumulNminfoDfNewPath)  // 누적사전 변경으로 인해 경로 변경 시 사용
    } else {
      fileFuncCls.rParquet(filePathClsBf1m.cumulNminfoDfPath)
    }
    val cumulHj = getCumulHj(cumulNminfoDf, hjItem)

    // 아이템을 부착하기 위해, 아이템명과 사이즈를 정제 후, HJ 모집단 아이템 기반 누적사전과 JOIN 하여 데이터 추출
    val baseWithCumulHj = getBaseWithCumulHj(base, cumulHj)
    // 업체별 상호명, 주소, 모집단에 따른 아이템 매입금액 추출
    val moAddr = getMoAmt(baseWithCumulHj)
    moAddr
  }
}