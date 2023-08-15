/**
 * HJ 마트 생성

 */
package sool.service.get_marts.get_mart_hj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, sum, when, udf}
import sool.service.run.RunService.logger
import sool.common.path.FilePath
import sool.common.jdbc.JdbcGet
import sool.service.get_marts.get_mart_hj._

class RunGetMartHj(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._
  import scala.util.matching.Regex
  import scala.collection.mutable.ListBuffer

  val jdbcGetCls = new JdbcGet(spark)
  val saveMartHjCls = new SaveMartHj(spark)

  val ch3Func = udf((itemNmOrg: String)=>{
    val pattern1 = "[^;]*;|[^:]*:".r //바코드
    val pattern2 =  "03".r // 유흥 채널
    val pattern3 = "^0\\d{1,1};".r //0포함 2자리 숫자
    val dionyConv = pattern1.findAllIn(itemNmOrg).toList
    val add = if (dionyConv.length%2==0) 3 else 2
    var rst = new ListBuffer[Int]()
    // 주종이 먼저 나왔을 경우(0으로 시작하는 숫자2자리) 0번째부터 03채널 찾기
    if (pattern3.findAllIn(dionyConv(0)).toArray.length>0){
      for(i <-0 until dionyConv.length by add){
        if (pattern2.findAllIn(dionyConv(i)).toArray.length>0){
          rst += 1
        }
        else rst +=0
      }
      rst.sum == 0
    }
    // 바코드가 나온 경우 1번째부터 03채널 찾기
    else{
      for(i <- 1 until dionyConv.length by add){
        if (pattern2.findAllIn(dionyConv(i)).toArray.length>0){
          rst += 1
        }
        else rst +=0
      }
      rst.sum == 0
    }
  })


  def getHjDfs(ethDt:String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)

    // DB, HJ 맥주, 소주 아이템 마스터
    val hjbItem = jdbcGetCls.getItemTbl("HJ_BR_ITEM")
    val hjsItem = jdbcGetCls.getItemTbl("HJ_SJ_ITEM")

    // FIRST_TRADE_LIST1
    val firstTradeList = spark.read.parquet(filePathCls.hjFirstTradeListPath)

    // SA_UP_JANG
    val saUpJang = spark.read.parquet(filePathCls.hjSaUpJangPath)

    // com_main
    val comMain = spark.read.parquet(filePathCls.comMainPath)

    // base_tagged
    val baseTaggedPre = spark.read.parquet(filePathCls.baseTaggedPath)

    val itemUDF = udf((rawItem: String) => {
      val item = rawItem.replace(":", ";").replace(" ", "")
      val definePleasureRegex1 = ";0?3;\\d{0,2};"
      val definePleasureRegex2 = ";;\\d{0,2};.*(?=유흥)"
      val definePleasureRegex3 = ";0?3;\\d{1,2}[가-힣a-zA-Z;]"
      val definePleasureRegex = (definePleasureRegex1 + "|" + definePleasureRegex2 + "|" + definePleasureRegex3).r
      val definePleasure = definePleasureRegex.findAllIn(item).size
      if (definePleasure > 0){
        "Y"
      }else{
        "N"
      }
    })

    val baseTagged = baseTaggedPre.withColumn("YN", itemUDF('ITEM_NM)).filter($"YN" === "Y")

    (hjbItem, hjsItem, firstTradeList, saUpJang, comMain, baseTagged)
  }
  def getMartHjNonAddr(hjbItem:DataFrame,
                       hjsItem:DataFrame,
                       baseTagged:DataFrame,
                       firstTradeList:DataFrame,
                       saUpJang:DataFrame) = {
    /* 1) 말단 아닌 업체의 매입거래 제거 */
    //### Set 1 : (받은) HJ제조장 직접거래처 리스트
    val firstTradeList1 = firstTradeList.select("COM_RGNO").distinct

    //### Set 2 : (거래정보로 뽑은) HJ제조장 직접거래처 리스트
    val firstTradeList2 = baseTagged.
      join(saUpJang, baseTagged("SUP_RGNO") === saUpJang("COM_RGNO")).
      select('BYR_RGNO.as("COM_RGNO")).distinct

    //### Append Set (블랙리스트): 비소매업종업체 + 공급업체 + HJ제조장(파일) + 제조장거래처리스트1(파일) + 제조장거래처리스트2(로직)
    val blackList = saUpJang.
      union(firstTradeList1).
      union(firstTradeList2).
      withColumnRenamed("COM_RGNO", "BYR_RGNO").
      withColumn("BLACK", lit("YES"))

    //### 블랙리스트의 매입거래 제거
    val retailEdge = baseTagged.
      join(blackList, Seq("BYR_RGNO"), "left_outer").
      filter('BLACK.isNull).
      drop("BLACK")

    /* 2) HJ 맥주, 소주 관련 거래만 추출 */
    val edgeHjb = retailEdge.join(hjbItem, retailEdge("ITEM") === hjbItem("BR_NM"), "inner")
    val edgeHjs = retailEdge.join(hjsItem, retailEdge("ITEM") === hjsItem("SJ_NM"), "inner")

    /* 3) 사이즈 보정 (SIZE가 없는 경우 DTI의 ITEM_SZ로 대체) */
    val martHjbNonAddr = edgeHjb.
      withColumn("QT", 'ITEM_QT * 'VSL_SIZE).
      withColumnRenamed("BYR_RGNO", "COM_RGNO")
    val martHjsNonAddr = edgeHjs.
      withColumn("QT", 'ITEM_QT * 'VSL_SIZE).
      withColumnRenamed("BYR_RGNO", "COM_RGNO")
    (martHjbNonAddr, martHjsNonAddr)
  }

  def getMartHjWithAddr(martHjNonAddr:DataFrame, comMain:DataFrame, itemTbl:DataFrame) = {
    // comMainAddr
    val comMainAddrPre1 = comMain.
      select("COM_RGNO", "ADDR1", "ADDR2", "ADDR3", "ADDR3D", "ADDR3R", "POST_CD").
      na.fill("전체", Seq("ADDR1","ADDR2","ADDR3", "ADDR3D", "ADDR3R", "POST_CD"))
    val comMainAddrPre2 = comMainAddrPre1.
      withColumn("ADDR2", when('ADDR1.startsWith("세종특별자치시"),"전체").otherwise('ADDR2)).
      filter(!('ADDR1.startsWith("전체") || 'POST_CD.startsWith("전체")))
    val comMainAddr = comMainAddrPre2.
      withColumn("ADDR0", lit("0전국")).
      withColumnRenamed("COM_RGNO", "dfAddr_COM_RGNO")

    // supAddr
    val supAddrPre1 = martHjNonAddr.
      join(comMainAddr, martHjNonAddr("SUP_RGNO") === comMainAddr("dfAddr_COM_RGNO"))
    val supAddrPre2 = supAddrPre1.
      withColumnRenamed("ADDR0", "ADDR0_SUP").
      withColumnRenamed("ADDR1", "ADDR1_SUP").
      withColumnRenamed("ADDR2", "ADDR2_SUP").
      withColumnRenamed("ADDR3", "ADDR3_SUP").
      withColumnRenamed("ADDR3D", "ADDR3D_SUP").
      withColumnRenamed("ADDR3R", "ADDR3R_SUP").
      withColumnRenamed("POST_CD", "POST_CD_SUP")
    val supAddr = supAddrPre2.
      select("SUP_RGNO", "ADDR0_SUP", "ADDR1_SUP", "ADDR2_SUP", "ADDR3_SUP",
        "ADDR3D_SUP", "ADDR3R_SUP", "POST_CD_SUP").
      distinct

    val supByrItemAgg = martHjNonAddr.groupBy('SUP_RGNO, 'COM_RGNO, 'ITEM).
      agg(sum('ITEM_QT).alias("SUM_ITEM_QT"), sum('QT).alias("SUM_QT"), sum('SUP_AMT).alias("SUM_SUP_AMT"))

    val addrJoin1 = supByrItemAgg.
      join(supAddr, Seq("SUP_RGNO"), "left_outer" ).
      na.fill("주소없음", Seq("ADDR1_SUP", "ADDR2_SUP", "ADDR3_SUP", "ADDR3D_SUP", "ADDR3R_SUP", "POST_CD_SUP"))

    val addrJoin2 = addrJoin1.
      join(comMainAddr, supByrItemAgg("COM_RGNO") === comMainAddr("dfAddr_COM_RGNO"), "inner").
      drop("dfAddr_COM_RGNO")

    val itemTblCols = itemTbl.columns
    val itemTblColNm = if (itemTblCols.contains("BR_NM")) "BR_NM" else if (itemTblCols.contains("SJ_NM")) "SJ_NM" else ""
    val martHjWithAddr = addrJoin2.join(itemTbl, addrJoin2("ITEM") === itemTbl(itemTblColNm), "inner")
    martHjWithAddr
  }

  def runGetMartHj(ethDt:String, flag: String) = {
    logger.info("[appName=sool] [function=runGetMartHj] [runStatus=start] [message=start]")

    // 데이터 준비
    val (hjbItem, hjsItem, firstTradeList, saUpJang, comMain, baseTagged) = getHjDfs(ethDt, flag)

    // 주소 없는 HJ 맥주, 소주 마트 생성
    val (martHjbNonAddr, martHjsNonAddr) = getMartHjNonAddr(hjbItem, hjsItem, baseTagged, firstTradeList, saUpJang)
    saveMartHjCls.saveMartHjNonAddr(martHjbNonAddr, martHjsNonAddr, ethDt, flag)  // 저장

    // 주소 포함 최종 HJ 맥주, 소주 마트 생성
    val martHjb = getMartHjWithAddr(martHjbNonAddr, comMain, hjbItem)
    val martHjs = getMartHjWithAddr(martHjsNonAddr, comMain, hjsItem)
    saveMartHjCls.saveMartHj(martHjb, martHjs, ethDt, flag)  // 저장

    logger.info("[appName=sool] [function=runGetMartHj] [runStatus=end] [message=end]")
  }
}
