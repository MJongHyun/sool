name := "sool"

version := "0.4.27"

scalaVersion := "2.11.12"

// 자체 개발 빌드
// libraryDependencies += ".kw" % "fi_rate" % "1.0.0"
// resolvers += Resolver.mavenLocal
// 오프라인시
// offline := true
assemblyJarName in assembly := "sool.jar"
// additional libraries
// 신규 주류 서버에 하둡 라이브러리와의 jar 파일 내에 라이브러리 랑 충돌이 발생하여 s3 가 사용이 안돼서 "provided" 를 추가함
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.apache.spark" % "spark-core_2.11" % "2.4.7" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.7" % "provided" ,
  "com.crealytics" % "spark-excel_2.11" % "0.13.1",
  "mysql" % "mysql-connector-java" % "5.1.38",
  //  "mysql" % "mysql-connector-java" % "8.0.21",
  "org.jsoup" % "jsoup" % "1.11.2",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.3.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2"
//  "org.scalaj" % "scalaj-http_2.11" % "2.4.2",
//  "org.apache.httpcomponents" % "httpclient" % "4.5"
  //  "org.apache.spark" % "spark-mllib_2.11" % "2.4.7" % "provided" ,
  //  "org.apache.spark" % "spark-repl_2.11" % "2.4.7" % "provided" ,
  //  "org.apache.spark" % "spark-yarn_2.11" % "2.4.7" % "provided" ,
)


assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case _ => MergeStrategy.first
}
