/**
 * jdbc Properties

 */
package sool.common.jdbc

import sool.common.check_args.CheckIp
import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

class JdbcProp {
  // host IP
  val hostIp = new CheckIp().getHostIp()

  // 술 DB
  def soolDbInfo() = {
    val user = ""
    val password = ""
    val url = if (hostIp == "0.0.0.0") {  // 신규 주류 서버 실행 시
      "jdbc:mysql://127.0.0.1:3306/LIQUOR?serverTimezone=UTC&autoReconnect=true"
    } else {
      "jdbc:mysql://0.0.0.0:3306/LIQUOR?serverTimezone=UTC&autoReconnect=true"
    }
    (user, password, url)
  }

  // 술 주소 DB
  def soolAdrsDbInfo() = {
    val user = ""
    val password = ""
    val url = if (hostIp == "0.0.0.0") {  // 신규 주류 서버 실행 시
      "jdbc:mysql://127.0.0.1:3306/ADDRESS?serverTimezone=UTC&autoReconnect=true"
    } else {
      "jdbc:mysql://0.0.0.0:3306/ADDRESS?serverTimezone=UTC&autoReconnect=true"
    }
    (user, password, url)
  }

  def soolBldngInfo(){
    val user = ""
    val password = ""
    val url = if (hostIp == "0.0.0.0") {  // 신규 주류 서버 실행 시
      "jdbc:mysql://127.0.0.1:3306/ADDRESS?serverTimezone=UTC&autoReconnect=true"
    } else {
      "jdbc:mysql://0.0.0.0:3306/ADDRESS?serverTimezone=UTC&autoReconnect=true"
    }
    (user, password, url)
  }

  def clickHouseStat80() = {
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    var conn: Connection = null
    val ckProperties = new Properties()
    ckProperties.put("user", "default")
    ckProperties.put("password", "!@1234")

    val jdbcUrl = "jdbc:clickhouse://211.180.114.66:8123/CLCTDATA"
    conn = DriverManager.getConnection(jdbcUrl, ckProperties)
    val stat = conn.createStatement()

    (conn, stat, jdbcUrl, ckProperties)
  }

  // DK TEST DB
  def dkTestDbInfo() = {
    val user = "sool_dk_user" // "root"
    val password = "sool_dk_user01" // "!Root@1234"
    val url = if (hostIp == "0.0.0.0") {  // 신규 주류 서버 실행 시
      "jdbc:mysql://172.31.15.65:3306/sool_dk_db_dev?useSSL=false&serverTimezone=UTC&autoReconnect=true&rewriteBatchedStatements=true"  // new_sool 서버에서 dk_test 서버 DB 접근할 떄 사용
    } else {
      "jdbc:mysql://scp-sool-dk.czibagr5c3hk.ap-northeast-2.rds.amazonaws.com/sool_dk_db_dev"
    }
    (user, password, url)
  }

  // connProp
  def getConnProp(user: String, password: String) = {
    val connProp = new java.util.Properties()
    connProp.put("user", user)
    connProp.put("password", password)
    connProp.put("driver", "com.mysql.jdbc.Driver")
    connProp
  }

  // 술 DB jdbcProp
  def jdbcProp() = {
    val (user, password, url) = soolDbInfo()
    val connProp = getConnProp(user, password)
    (url, connProp)
  }

  // DK TEST DB jdbcProp
  def jdbcPropDkTest() = {
    val (user, password, url) = dkTestDbInfo()
    val connProp = getConnProp(user, password)
    (url, connProp)
  }

  // 술 주소 DB jdbc statement
  def soolAdrsJdbcStmt() = {
    val driver = "com.mysql.jdbc.Driver"
    val (user, password, url) = soolAdrsDbInfo()
    var conn: Connection = null
    Class.forName(driver)
    conn = DriverManager.getConnection(url, user, password)
    val stmt = conn.createStatement()
    (conn, stmt)
  }

  // DK TEST 서버 jdbc statement
  def dkTestDbJdbc() = {
    val driver = "com.mysql.jdbc.Driver"
    val (user, password, url) = dkTestDbInfo()
    var conn: Connection = null
    Class.forName(driver)
    conn = DriverManager.getConnection(url, user, password)
    val stmt = conn.createStatement()
    (conn, stmt)
  }

  // DB 연결 끊기
  def closeDbJdbc(conn: java.sql.Connection, stmt: java.sql.Statement) = {
    stmt.close()
    conn.close()
  }
}
