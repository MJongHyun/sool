/**
 * date 인자값 타입 체크, 집계연월 체크하는 클래스

 */
package sool.common.check_args

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class CheckDt {
  // targetDt가 숫자로만 구성되어 있는지 확인하기
  def checkDtTypeInt(targetDt:String) = {
    val targetDtSize = targetDt.size
    val intCntInTargetDt = targetDt.filter(_.getType == 9).size
    if (intCntInTargetDt == targetDtSize) {
      true
    } else {
      println("입력한 연월에 숫자가 아닌 값이 포함되어 있습니다.")
      false
    }
  }

  // 인자값이 YYYYMM 형태인지 체크하는 기능
  def checkYMFrmt(targetDt:String) = {
    try {
      val dtFrmt = DateTimeFormat.forPattern("yyyyMM")
      dtFrmt.parseDateTime(targetDt)
      true
    } catch {
      case ex:IllegalArgumentException => println(s"\n${ex}")
        println("입력한 연월이 날짜가 맞는지, YYYYMM 형태인지 확인하세요.")
        false
    }
  }

  // 집계 연월이 맞는지 체크하는 기능(현재는 info 목적으로 사용, 추후 Boolean 값을 사용할지 결정)
  def checkAggYM(ethDt:String) = {
    val todayDate = new DateTime()
    val aggDate = todayDate.minusMonths(1)

    val dtFrmt = DateTimeFormat.forPattern("yyyyMM")
    val todayYM = dtFrmt.print(todayDate)
    val aggYM = dtFrmt.print(aggDate)

    if (ethDt == aggYM){
      true
    } else {
      println(s"현재 연월 : ${todayYM}")
      println(s"예상 집계 연월 : ${aggYM}")
      println(s"실행 집계 연월 : ${ethDt}")
      false
    }
  }
}
