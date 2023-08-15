package sool.common.function

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.CloseableHttpResponse
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat



class Slack {
  // api url 선언 (본인 slack 봇의 webhook)
  val apiurl = "https://hooks.slack.com/services/"

  val cmnFuncCls = new CmnFunc()
  val logger = cmnFuncCls.getLogger()
    // http client 생성
  val client = HttpClients.createDefault()

  def sendMsgFun(flag:String, task:String, file:String) = {
    val level = "INFO"
    val todayDate = new DateTime()
    val dtFrmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
    val startTime = dtFrmt.print(todayDate)
    // json 형식의 post data 생성
    val json =
      s"""{
         |	"blocks": [
         |		{
         |			"type": "section",
         |			"text": {
         |				"type": "mrkdwn",
         |				"text": "[${flag} : ${task}] \n${startTime}  `${level}`  ${file}"
         |			}
         |		}
         |	]
         |}""".stripMargin

    // post request 설정
    val post: HttpPost = new HttpPost(apiurl)
    post.addHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(json, "UTF-8"))

    // api url에 post request 후, 반환 값을 response에 저장
    val response: CloseableHttpResponse = client.execute(post)
    // response의 entity 추출
    val entity = response.getEntity

    // entity를 문자열로 변환
    val entity_str = EntityUtils.toString(entity, "UTF-8")

    entity_str
  }

  def sendErrorMsgFun(flag:String, task:String, file:String, msg:String) = {
    val level = "ERROR"
    val todayDate = new DateTime()
    val dtFrmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
    val startTime = dtFrmt.print(todayDate)
    // json 형식의 post data 생성
    val json =
      s"""{
         |	"blocks": [
         |		{
         |			"type": "section",
         |			"text": {
         |				"type": "mrkdwn",
         |				"text": "[${flag} : ${task}] \n${startTime}  `${level}`  ${file} \n Error Message: ${msg}"
         |			}
         |		}
         |	]
         |}""".stripMargin

    // post request 설정
    val post: HttpPost = new HttpPost(apiurl)
    post.addHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(json, "UTF-8"))

    // api url에 post request 후, 반환 값을 response에 저장
    val response: CloseableHttpResponse = client.execute(post)
    // response의 entity 추출
    val entity = response.getEntity

    // entity를 문자열로 변환
    val entity_str = EntityUtils.toString(entity, "UTF-8")

    entity_str
  }

  def sendMsg(flag:String, task:String, file:String)={
    var rst = sendMsgFun(flag:String, task:String, file:String)
    // 2번까지는 재 전송, 실패 시 log 남기기.
    if (rst != "ok") {
      rst = sendMsgFun(flag: String, task: String, file: String)
      if (rst != "ok") {
        logger.error(s"[file=Slack] [function=main] [status=error] [message=슬랙 전송 실패]")
      }
    }
  }

  def sendErrorMsg(flag:String, task:String, file:String, msg:String)= {
    var rst = sendErrorMsgFun(flag: String, task: String, file: String, msg: String)
    // 2번까지는 재 전송, 실패 시 log 남기기.
    if (rst != "ok") {
      rst = sendErrorMsgFun(flag: String, task: String, file: String, msg: String)
      if (rst != "ok") {
        logger.error(s"[file=Slack] [function=main] [status=error] [message=슬랙 전송 실패]")
      }
    }
  }
}