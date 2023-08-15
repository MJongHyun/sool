/**

 */
package sool.extract.extract_data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import sool.common.function.FileFunc
import sool.common.path.FilePath

class ExtrctDtiCh3(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  def getCh3Udf() = udf((rawItem: String) => {
    val item = rawItem.replace(":", ";").replace(" ", "") + " "
    val itemList = item.split(";")
    val itemListLength = itemList.size

    // Case1 (a)
    // (1) 구분자(;)로 텍스트 분리 후, 길이가 4인 경우, 맨 첫요소가 '바코드'가 아닌 '채널'의 형태(01|02|03|04)를 지닐 때(즉, 2자리 숫자로 구성),
    // 아이템 이름 그대로 가져옴, 아닌 경우 맨 첫요소 삭제(바코드 삭제)
    // (2) (1)에 해당되는 아이템 이름이 01; 또는 02; 또는 04;이 존재하고 그 뒤에 숫자 0~2자리+; 이 존재하는 경우
    // ("0(1|2|4);\\d{0,2};"), '비유흥'(N) 아니면 '유흥'(Y1) 처리

    // Case2 (_)
    // 구분자(;)로 텍스트 분리 후, 길이가 4개 요소 미만 또는 초과인 경우는 ";03;" 라는 부분문자열이 포함되면 '유흥'(Y2|Y3) 아니면 '비유흥'(N) 처리
    var channelResult = itemListLength match {
      case a if (itemListLength == 4) =>
        if ("0(1|2|4);\\d{0,2};".r.findAllIn(
          if (item.split(";")(0).matches("^0(1|2|3|4)")) item
          else item.split(";").drop(1).mkString(";") ).size > 0 ) "N"
        else "Y1"
      case _ => {
        if (itemListLength < 4) {
          item match {
            case a if (";03;".r.findAllIn(item).size > 0) => "Y2"
            case _ => "N"
          }
        } else {
          item match{
            case a if (";03;".r.findAllIn(item).size > 0) => "Y3"
            case _ => "N"
          }
        }
      }
    }

    // Case1(Y1) 경우 중, 채널자리에 값 없는데, "가정" 키워드 들어가면 유흥에서 제외
    // 아래 코드 추가 전/후 비교확인했음.
    // 채널값 없이 "가정" 키워드 들어간 경우는 *총거래건수: 1013건, *총거래금액: 189,194,947원, *총품목수: 152개
    // channelResult = if ( (channelResult == "Y1") && ("^;\\d{0,2};.{0,2}[^;]+(가정).*".r.findAllIn( if (item.split(";")(0).matches("^0(1|2|3|4)")) item else item.split(";").drop(1).mkString(";") ).size > 0) ) "N1" else channelResult
    channelResult =
      if ((channelResult == "Y1") && (item.replace(":", ";").matches("[^;]*;[^(03)]{0,2};[^;]{0,2};[^;]*(가정).*"))) "N1"
      else channelResult

    channelResult
  })

  // 유흥 데이터 추출
  def getDtiCh3(dtiRetail: DataFrame) = {
    val ch3Udf = getCh3Udf()
    val dtiRetailAddCol = dtiRetail.withColumn("CHANNEL", ch3Udf('ITEM_NM))
    val dtiCh3 = dtiRetailAddCol.filter('CHANNEL.contains("Y")).drop("CHANNEL")
    dtiCh3
  }

  // 메인
  def extrctDtiCh3(ethDt: String, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val dtiRetail = fileFuncCls.rParquet(filePathCls.dtiRtlPath)
    val dtiCh3 = getDtiCh3(dtiRetail)
    fileFuncCls.wParquet(dtiCh3, filePathCls.dtiCh3Path)  // 저장
  }
}
