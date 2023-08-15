/**
 * HK 마트 생성

 */
package sool.service.get_marts.get_mart_hk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import sool.service.run.RunService.logger
import sool.common.path.FilePath
import sool.common.jdbc.JdbcGet
import sool.service.get_marts.get_mart_hk._

class RunGetMartHk(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val jdbcGetCls = new JdbcGet(spark)
  val saveMartHkCls = new SaveMartHk(spark)

  // 필요 데이터 로드
  def getHkDfs(ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val hkMstr = jdbcGetCls.getHkAgrnItemTbl("HK_AGRN_ITEM")  // DB, HK 마스터

    // com_main
    val comMainPre = spark.read.parquet(filePathCls.comMainPath)
    val comMain = comMainPre.
      filter('ADDR1.isNotNull).
      select('COM_RGNO as "BYR_RGNO", 'ADDR1, 'ADDR2)

    // base_tagged
    val baseTaggedPre = spark.read.parquet(filePathCls.baseTaggedPath)
    val baseTagged = baseTaggedPre.
      filter('BYR_RGNO =!= "6028126576").   // 이스턴 마린 업체 제거 요청 [2020.05.19 HK]
      filter(!'ITEM_NM.contains(";;05;"))   // 채널비운 주종 05 제거 [2020.10.23 HK]

    // 고래맥주창고 테이블(2021.09.06 추가)
    // 고래맥주창고 사용여부 컬럼 추가로 Y만 추출(2022.11.22)
    val whaleBrComTbl = jdbcGetCls.getLiquorTbl("HK_WHALE_BEER_COM")
    val whaleBrComRgno = whaleBrComTbl.filter($"USE_YN"==="Y").select('COM_RGNO)
    (hkMstr, comMain, baseTagged, whaleBrComRgno)
  }

  // HK 마트 생성
  def getMartHk(hkMstr:DataFrame, comMain:DataFrame, baseTagged:DataFrame, whaleBrComRgno:DataFrame) = {
    val hkMstrRnmCol= hkMstr.withColumnRenamed("ITEM_NM", "ITEM") // baseTagged 랑 컬럼명 같게 하기
    val martHkPre1 = baseTagged.join(hkMstrRnmCol, Seq("ITEM"))
    val martHkPre2 = martHkPre1.join(comMain, Seq("BYR_RGNO"), "left_outer").filter('ADDR1.isNotNull)
    val martHkPre3 = martHkPre2.
      withColumn("ADDR0", lit("전국")).
      withColumn("QT", 'ITEM_QT * 'VSL_SIZE)

    // 매입업체에서 고래맥주창고 업체들 제거(2021.09.06 추가)
    val whaleBrByrRgno = whaleBrComRgno.select('COM_RGNO.as("BYR_RGNO"))  // 조인할 때 안전하게 하기 위해 컬럼명 변경
    val martHk = martHkPre3.join(whaleBrByrRgno, Seq("BYR_RGNO"), "leftAnti")   // BYR_RGNO 에서 고래맥주창고 관련 업체 제거
    martHk
  }

  // 메인
  def runGetMartHk(ethDt:String, flag: String) = {
    logger.info("[appName=sool] [function=runGetMartHnk] [runStatus=start] [message=start]")

    // 데이터 준비
    val (hkMstr, comMain, baseTagged, whaleBrComRgno) = getHkDfs(ethDt, flag)

    // HK 마트 생성
    val martHk = getMartHk(hkMstr, comMain, baseTagged, whaleBrComRgno)
    saveMartHkCls.saveMartHk(martHk, ethDt, flag)

    logger.info("[appName=sool] [function=runGetMartHnk] [runStatus=end] [message=end]")
  }
}