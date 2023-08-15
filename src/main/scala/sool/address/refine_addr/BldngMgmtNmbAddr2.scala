/**

 */

package sool.address.refine_addr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

class BldngMgmtNmbAddr2(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._
  
  // 필요 클래스 선언
  val scrapingAddrCls = new ScrapingAddr2()

  // 다음 API 수집 실행 udf
  def scrapingAddrUdf = udf((comAddr: String) => {
    try {
      if (comAddr == null || comAddr == "" || comAddr == " ") {
        println("*** 원천 주소 값이 없는 경우입니다. ***")
        println(comAddr)
        println("*** 원천 주소 값이 없는 경우입니다. ***")
        ("", 0)
      } else {
        scrapingAddrCls.runScrapingAddr(comAddr)
      }
    } catch {
      case ex: Exception => {
        println("*** 에러가 발생했습니다. ***")
        println(comAddr)
        println(ex)
        println("*** 에러가 발생했습니다. ***")
        ("", 0)
      }
    }
  })

  // 다음 API 를 통해 각 원천 주소에 해당하는 건물관리번호 구하기
  def getBldngMgmtNmbAddr(rawAddr: DataFrame) = {
    // 원천주소 trim 처리
    val rawAddrTrimUdf = udf((comAddr: String) => if (comAddr == null || comAddr == "" || comAddr == " ") comAddr else comAddr.trim())
    val rawAddrTrim = rawAddr.withColumn("COM_ADDR", rawAddrTrimUdf('COM_ADDR))   // 원천주소 trim 처리
    // 원천 주소별 건물관리번호 수집
    val bldngMgmtNmbAddr = rawAddrTrim.withColumn("SCRAPING_ADDR", scrapingAddrUdf('COM_ADDR))
    bldngMgmtNmbAddr
  }
  
  // 최종 건물관리번호, PNU, 원천주소, 검색결과개수 데이터프레임 구하기
  def getBldngMgmtNmbAddrForDb(bldngMgmtNmbAddrPrqt:DataFrame) = {
    // DB Upsert 시 필요한 컬럼 생성
    val bldngMgmtNmbAddrForDbPre1 = bldngMgmtNmbAddrPrqt.select(
      col("SCRAPING_ADDR._1").as("BLDNG_MGMT_NMB"),
      col("SCRAPING_ADDR._1").substr(0, 19).as("PNU"),
      col("COM_ADDR").as("SRC_ADRS"),
      col("SCRAPING_ADDR._2").as("SRCH_RSLTS_CNT")
    )
    val bldngMgmtNmbAddrForDbPre2 = bldngMgmtNmbAddrForDbPre1.filter('BLDNG_MGMT_NMB =!= "" && 'SRCH_RSLTS_CNT > 0)

    /* DB Upsert 시 에러 발생 상황 해결
    SRC_ADRS 에 "\" 문자가 들어가면 sql 문법 에러가 발생하기 때문에 아래와 replace 처리한다.
    ex: "서울특별시 노원구 동일로243길 52, 1층(상계동)\", "서울 마포구 동교로38안길 24 1층 (연남동, 연남 Hong's Ville)"
    + 원천 주소에 좌우 공백은 제거한다. 공백 형태를 띈 특수문자는 제거하지 않는다.
     */
    val srcAddrRplcUdf = udf((srcAdrs: String) => {
      srcAdrs.replace("\\", "\\\\")
    })
    val bldngMgmtNmbAddrForDb = bldngMgmtNmbAddrForDbPre2.withColumn("SRC_ADRS", srcAddrRplcUdf(col("SRC_ADRS")))
    bldngMgmtNmbAddrForDb
  }
}