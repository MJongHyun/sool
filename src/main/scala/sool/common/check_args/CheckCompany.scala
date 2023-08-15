/**

 */
package sool.common.check_args

class CheckCompany {
  // 서비스 제공 회사에 해당되는지 확인
  def checkCompany(targetCompany:String) = {
    val companyList = List("HJ", "DK", "HK")
    companyList.contains(targetCompany)
  }
}
