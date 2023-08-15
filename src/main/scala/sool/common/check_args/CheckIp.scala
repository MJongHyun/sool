/**
 * IP 확인 및 코드 사용 권한 설정

 */
package sool.common.check_args

import java.net.InetAddress

class CheckIp {
  // IP
  // todo:  로컬에서 계속 172.0.0.1 로 뜨는 현상 해결하기
  def getHostIp() = InetAddress.getLocalHost().getHostAddress

  // 코드 사용 권한 IP 리스트
  def getIpList() = {
    val ipList = Seq(
      "127.0.0.1" // temp
    )
    ipList
  }

  // 코드 사용 권한 IP 확인
  def checkIp() = {
    val hostIp = getHostIp()
    val ipList = getIpList()
    if (ipList.contains(hostIp)) {
      println(s"IP: ${hostIp}")
      true
    } else {
      println("코드 실행이 허가되지 않은 IP 입니다.")
      false
    }
  }
}
