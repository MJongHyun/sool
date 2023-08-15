/**
 * 최종 품목 사전(item_info_AFTER.parquet) 생성

 */
package sool.service.item_tagging_bf

import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{abs, col, lit, udf, when}
import sool.common.path.FilePath

class RunGetItemInfoAfter(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)

  // 비모집단 아이템 추출 udf
  def getNonItemUdf() = {
    val nonItemUDF = udf((itemNm: String) => {
      val keyWine1 = itemNm.matches(".*(소비뇽|쇼비뇽|샤또|까쇼|DOC|IGT|VDT|클라시코|리제르바|세코|비앙코|돌체|레드와인|화이트와인|피노누아|홉노브|까르메네|몽블랑|롱독|템프라|세구라|케이머스|까바|프론테라|타파스|빌라엠).*")
      val keyWine2 = itemNm.matches(".*(캔달잭슨|켄달잭슨|샤도네이|샤르도네|샴페인|루이로드레).*")
      val keyJunmy1 = itemNm.matches(".*(사케|쥰마이|준마이|송죽매|오제끼|후쿠쥬|오니고로|간바레|요하찌|하쿠쯔|히카리|핫카이|하쿠시|탄레이|키쿠마|쿠보타|쿠로마|카라탄|카라구|텐구|오토코야|아마구치).*")
      val keyJunmy2 = itemNm.matches(".*(센노유|상선|무진구|반슈니|죠센|나마죠|월계관|다이긴).*")
      val keyChina = itemNm.matches(".*(공부가|연태|고려촌|고량주|이과두|이과도|금화천|노주탄|소홍주|소흥주|수정방|대만죽엽|만만춘|양하대곡|마오타이|천진).*")
      val keyMak = itemNm.matches(".*(막걸리).*")
      val keyBrandy = itemNm.matches(".*(브랜디|VSOP|헤네시|레미마).*")
      val keyLiq = itemNm.matches(".*(베일리스|코인트|볼스크림|볼스멜론|볼스트리|큐라소|애플퍼커|애플파커|아구아|아그와|코인트로|드람브|디사론|디카이퍼|아마레또|깔루아|말리부|미도리|힙노틱|트리플섹|크림드|엑스레|디카이퍼).*")
      val keyTonic = itemNm.matches(".*(고든스|고든진|마티니드라이|마티니엑스트라|비피터|탕크레이|탕크레이|탱거레이|탱커레이|탱그레이|탱크레이).*")
      val keyVodka = itemNm.matches(".*(앱솔|엡솔|압솔|버젤페터|압생트|핀란디아|넵머이|레퍼드레어).*")
      val keyRum = itemNm.matches(".*(더럼|바카디|화이트럼|하바나클|아뇨스).*")
      val keyTequila = itemNm.matches(".*(테남파|호세|시에라데).*")
      // val keySham = itemNm.matches(".*(샴페인|루이로드레).*")
      val keyEtc = itemNm.matches(".*(매실원|미림).*")

      itemNm match {
        case a11 if (keyWine1 == true) => "비모집단와인"
        case a12 if (keyWine2 == true) => "비모집단와인"
        case a21 if (keyJunmy1 == true) => "비모집단기타주류일본술"
        case a22 if (keyJunmy2 == true) => "비모집단기타주류일본술"
        case a3 if (keyChina == true) => "비모집단기타주류중국술"
        case a4 if (keyMak == true) => "비모집단탁주"
        case a51 if (keyBrandy == true) => "비모집단기타주류"
        case a52 if (keyLiq == true) => "비모집단기타주류"
        case a53 if (keyTonic == true) => "비모집단기타주류"
        case a54 if (keyVodka == true) => "비모집단기타주류"
        case a55 if (keyRum == true) => "비모집단기타주류"
        case a56 if (keyTequila == true) => "비모집단기타주류"
        // case a57 if (keySham == true) => "비모집단기타주류"
        case a59 if (keyEtc == true) => "비모집단기타주류"
        case _ => ""
      }
    })
    nonItemUDF
  }

  // 신규 품목사전 생성 (비모집단 아이템 추출, CHECK 컬럼 등 추가)
  def getNewInfo(itemInfoBf: DataFrame, dkWskItem: DataFrame) = {
    // 비모집단 아이템 추출 udf
    val nonItemUdf = getNonItemUdf()

    // 안 본 아이템들에 대해 비모집단 아이템 추출, "NON_ITEM" 컬럼 생성
    val nonItemDfPre1 = itemInfoBf.select("SAW", "NAME", "ITEM_SZ").filter('SAW === "안봄")
    val nonItemDfPre2 = nonItemDfPre1.withColumn("NON_ITEM", nonItemUdf($"NAME"))
    val nonItemmDf = nonItemDfPre2.filter('NON_ITEM =!= "").select('NAME, 'ITEM_SZ, 'NON_ITEM).distinct

    // 신규 품목사전 생성
    // 위스키 아이템 추출(위스키는 DK 에만 있으니 DK 만 사용하면 된다.)
    val wskItem = dkWskItem.select('WSK_NM).collect.map(_(0)).toList
    val newInfoPre1 = itemInfoBf.withColumn("ITEM_ORG", 'ITEM)
    val newInfoPre2 = newInfoPre1.join(nonItemmDf, Seq("NAME", "ITEM_SZ"), "left")
    val newInfo = newInfoPre2.
      withColumn("MEMO", lit("")).
      withColumn("CHECK",
        when(abs('AMT) >= 500000.0 && 'SAW === "안봄", lit("Y")).
        when(('ITEM_SZ <= 200.0 ||
          ('ITEM_SZ >= 330.0 && 'ITEM_SZ <= 375.0) ||
          ('ITEM_SZ >= 473.0 && 'ITEM_SZ <= 560.0) ||
          'ITEM_SZ === 640.0 || 'ITEM_SZ === 700.0 ||
          'ITEM_SZ >= 1000.0) && 'SAW === "안봄", lit("Y")). // 위스키 관련 사이즈 추가 [2020.10.05], 신제품 진로미니팩 추가로 인한 사이즈 추가 [2021.01.11]
        when('NAME.contains("진로") && 'SAW === "안봄", lit("Y")). // 신제품 진로미니팩 추가로 인한 이름 추가 [2021.01.11]
        when('NAME.contains("HK") && 'SAW === "안봄", lit("Y")).
        when('ITEM.isin(wskItem:_*) && 'SAW === "안봄", lit("Y")). // 태깅 결과가 위스키로 나온값 확인 [20200909 추가]
        when('TYPE.contains("09") && 'SAW === "안봄", lit("Y")). // 아이템에서 주종이 위스키가 포함된 값 확인 [20200909 추가]
        when((('TYPE =!= "06" || 'ITEM_SZ =!= 750.0) &&
          ('TYPE =!= "04" || 'ITEM_SZ =!= 720.0) &&
          ('TYPE =!= "04" || 'ITEM_SZ =!= 900.0)) && 'SAW === "안봄", lit("Y")).
        // 주종이 과실주(06) 이면서 사이즈가 750인 경우, 주종이 청주(04) 이면서 사이즈가 720 또는 900 인 경우 제외 (와인 특징 및 준마이 특징) [2020.10.07]
        otherwise(null)
      )
    newInfo
  }

  // 최종 품목사전 저장 --> 매뉴얼 태깅할 파일 생성
  def getItemInfoAf(newInfo: DataFrame) = {
    val itemInfoAfPre = newInfo.
      withColumn("SAW", when($"SAW" === "봄", lit(null)).otherwise($"SAW"))
    val itemInfoAfCols = Seq(
      "BARCODE", "OLD_NEW", "TYPE", "NAME", "ITEM_SZ",
      "ITEM_ORG", "MOST_ITEM", "NON_ITEM", "ITEM", "SAW",
      "MEMO", "AMT", "CHECK"
    ).map(col)
    val itemInfoAf = itemInfoAfPre.select(itemInfoAfCols:_*).orderBy('ITEM.asc, 'NAME.asc, 'ITEM_SZ.asc)
    itemInfoAf
  }

  // item_info_BEFORE 의 총 개수와 itemInfoAf 의 총 개수 비교
  def checkItemInfoBfAfCnt(itemInfoBf: DataFrame, itemInfoAf: DataFrame) = {
    val checkBoolean = itemInfoBf.count == itemInfoAf.count
    if (checkBoolean) {
      println(s"${getTimeCls.getTimeStamp()}, item_info_BEFORE 의 총 개수와 itemInfoAf 의 총 개수가 같습니다. 이상 없습니다.")
    } else {
      println(s"${getTimeCls.getTimeStamp()}, item_info_BEFORE 의 총 개수와 itemInfoAf 의 총 개수가 다릅니다. 확인이 필요합니다.")
    }
    checkBoolean
  }

  // itemInfoAf 의 아이템명, 사이즈 별 중복 있는지 체크
  def checkItemInfoAf(itemInfoAf: DataFrame) = {
    val checkBoolean = itemInfoAf.count == itemInfoAf.select('NAME, 'ITEM_SZ).distinct.count
    if (checkBoolean) {
      println(s"${getTimeCls.getTimeStamp()}, itemInfoAf 의 아이템명('NAME) & 사이즈('ITEM_SZ) 에 중복이 존재하지 않습니다. 이상 없습니다.")
    } else {
      println(s"${getTimeCls.getTimeStamp()}, itemInfoAf 의 아이템명('NAME) & 사이즈('ITEM_SZ) 에 중복이 존재합니다.")
    }
    checkBoolean
  }

  def showInfoAboutItemInfoAf(itemInfoAf: DataFrame) = {
    itemInfoAf.groupBy("SAW").count.show(false) // 검토할 개수 (안봄)
    itemInfoAf.groupBy("CHECK").count.show(false) // 검토자 별 검토할 개수
    println("비모집단 아이템 검토 수 : " + itemInfoAf.filter('NON_ITEM.isNotNull).count)
  }

  // 메인
  def runGetItemInfoAf(ethDt: String, flag: String): Unit = {
    val filePathCls = new FilePath(ethDt, flag)

    /* 필요 데이터 준비 */
    val itemInfoBf = spark.read.parquet(filePathCls.itemInfoBfPath)
    val dkWskItem = jdbcGetCls.getItemTbl("DK_WSK_ITEM")

    // 신규 품목 사전
    val newInfo = getNewInfo(itemInfoBf, dkWskItem)

    // 최종 품목사전 저장 --> 매뉴얼 태깅할 파일 생성
    val itemInfoAf = getItemInfoAf(newInfo)

    /* 검토 */
    if (checkItemInfoBfAfCnt(itemInfoBf, itemInfoAf) == false || checkItemInfoAf(itemInfoAf) == false) return
    showInfoAboutItemInfoAf(itemInfoAf)  // 관련 정보 확인
    fileFuncCls.wParquet(itemInfoAf, filePathCls.itemInfoAfPath)  // 최종 품목사전 parquet 저장
    fileFuncCls.wXlsx(itemInfoAf, filePathCls.itemInfoAfXlxPath)  // 최종 품목사전 엑셀 저장
  }
}
