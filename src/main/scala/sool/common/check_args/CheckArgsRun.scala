/**
 * args 체크

 */
package sool.common.check_args

class CheckArgsRun {
  // RunService 오브젝트에서 필요한 값들을 불러오기 위해 만듦
  def checkServiceArgsVals(ethDt:String) = {
    val checkDtObj = new CheckDt()
    val checkIntCntInEthDtBool = checkDtObj.checkDtTypeInt(ethDt) // 숫자로만 구성되어 있는지 확인
    val checkYMFrmtBool = checkDtObj.checkYMFrmt(ethDt) // 날짜 형태 확인
    (checkIntCntInEthDtBool, checkYMFrmtBool)
  }

//  communication param 체크 (RunCommunication 오브젝트에서 사용)
  def checkCommunicationArgsVals(targetCompany:String) = {
    new CheckCompany().checkCompany(targetCompany)
  }
}
