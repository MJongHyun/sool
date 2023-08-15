/**
 * 신규 소매상 업데이트 및 최종 도매상 업데이트 후 파일 저장

 */
package sool.extract.extract_data

import org.apache.spark.sql.DataFrame
import sool.common.function.{FileFunc, GetTime}
import sool.common.path.FilePath
import sool.extract.run.RunExtract.logger

class ExtrctRetail(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()

  // 도매 제거(신규 공급 내역 업체 추가로 인한 기존 소매상 리스트에서 공급 내역 존재 업체가 없게 하기 위함)
  def getRetailBf1mExcptVndr(retailBf1m: DataFrame, vendor: DataFrame) = {
    val retailBf1mExcptVndr = retailBf1m.except(vendor).distinct.toDF("RGNO")
    retailBf1mExcptVndr
  }

  // 신규 매입자 추가
  def getNewCom(dtiCh3: DataFrame, retailRaw: DataFrame) = {
    val dtiCh3Byr = dtiCh3.select('BYR_RGNO).distinct
    val retailRawRnCol = retailRaw.select('RGNO.as("BYR_RGNO"))
    val newCom = dtiCh3Byr.except(retailRawRnCol)
    newCom
  }

  // 이번 달 최종 소매상 데이터 추출
  def getRetail(retailBf1mExcptVndr: DataFrame, newCom: DataFrame) = {
    val newComRnCol = newCom.select('BYR_RGNO.as("RGNO"))
    val retail = retailBf1mExcptVndr.union(newComRnCol)
    retail
  }

  // 메인
  def extrctRetail(ethDt: String, flag: String) = {
    val ethBf1m = getTimeCls.getEthBf1m(ethDt)
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)

    // 필요 데이터 로드
    val retailBf1m = fileFuncCls.rParquet(filePathClsBf1m.retailPath)
    val vendor = fileFuncCls.rParquet(filePathCls.vendorPath)
    val dtiCh3 = fileFuncCls.rParquet(filePathCls.dtiCh3Path)

    // 소매상 업데이트 1 (공금내역 존재업체 제거)
    val retailBf1mExcptVndr = getRetailBf1mExcptVndr(retailBf1m, vendor)
    logger.info(s"[file=ExtrctRetailAndVendor] [function=extrctRetailAndVendor] [status=running] " +
      s"[message=삭제 소매상 수: ${retailBf1m.except(retailBf1mExcptVndr).count}]")

    // 소매상 업데이트 2 (신규 매입자 추가)
    val newCom = getNewCom(dtiCh3, retailBf1mExcptVndr)
    logger.info(s"[file=ExtrctRetailAndVendor] [function=extrctRetailAndVendor] [status=running] " +
      s"[message=신규 매입자 수: ${newCom.count}]")
    fileFuncCls.wParquet(newCom, filePathCls.newComPath)  // 저장

    // 신규 소매상 업데이트 및 최종 도매상 업데이트 후 파일로 저장
    val retail = getRetail(retailBf1mExcptVndr, newCom)
    fileFuncCls.wParquet(retail, filePathCls.retailPath)
  }
}