package sool.extract.extract_data

import sool.common.function.{FileFunc, GetTime}
import org.apache.spark.sql.DataFrame
import sool.common.path.FilePath
import sool.extract.run.RunExtract.logger

class ExtractCumul(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()

  // 도매 제거(신규 공급 내역 업체 추가로 인한 기존 소매상 리스트에서 공급 내역 존재 업체가 없게 하기 위함)

  def extrctCumul(ethDt:String, flag:String) = {
    val ethBf2m = getTimeCls.getEthBf2m(ethDt)
    val filePathCls = new FilePath(ethDt, flag)
    val filePathClsBf2m = new FilePath(ethBf2m, flag)

    // dti 경로에서 2달 전 누적사전 불러오기
    val cumulNminfoDf = if (fileFuncCls.checkS3FileExist(filePathClsBf2m.dtiCumulNminfoDfNewPath)) {
      spark.read.parquet(filePathClsBf2m.dtiCumulNminfoDfNewPath)  // 누적사전 변경으로 인해 경로 변경 시 사용
    } else {
      spark.read.parquet(filePathClsBf2m.dtiCumulNminfoDfPath)
    }

    // 이번달 경로에 저장
    fileFuncCls.wParquet(cumulNminfoDf, filePathCls.invCumulNminfoDfPath)
  }
}
