/**
 * 특정 연월을 구할 때 사용

 */
package sool.common.function

import org.joda.time.format.DateTimeFormat

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class GetTime extends java.io.Serializable {
  // 전 달 구하는 함수
  def getEthBf1m(ethDt: String) = {
    val dtFrmt = DateTimeFormat.forPattern("yyyyMM")
    val ethBf1mDateTime = dtFrmt.parseDateTime(ethDt).minusMonths(1)
    val ethBf1m = dtFrmt.print(ethBf1mDateTime)
    ethBf1m
  }
  // 2달전 구하는 함수
  def getEthBf2m(ethDt: String) = {
    val dtFrmt = DateTimeFormat.forPattern("yyyyMM")
    val ethBf1mDateTime = dtFrmt.parseDateTime(ethDt).minusMonths(2)
    val ethBf1m = dtFrmt.print(ethBf1mDateTime)
    ethBf1m
  }
  // TimeStamp
  def getTimeStamp() = {
    val timeStamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)
    timeStamp
  }
  // 전 년도 구하는 함수
  def getEthBf1y(ethDt: String) = {
    val dtFrmt = DateTimeFormat.forPattern("yyyyMM")
    val ethBf1yDateTime = dtFrmt.parseDateTime(ethDt).minusYears(1)
    val ethBf1y = dtFrmt.print(ethBf1yDateTime)
    ethBf1y
  }
  // 다음 달 구하는 함수
  def getEthAf1m(ethDt: String) = {
    val dtFrmt = DateTimeFormat.forPattern("yyyyMM")
    val ethAf1mDateTime = dtFrmt.parseDateTime(ethDt).plusMonths(1)
    val ethAf1m = dtFrmt.print(ethAf1mDateTime)
    ethAf1m
  }
}
