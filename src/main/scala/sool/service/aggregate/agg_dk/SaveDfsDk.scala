/**

 */
package sool.service.aggregate.agg_dk

import org.apache.spark.sql.DataFrame
import sool.common.function.FileFunc
import sool.common.path.FilePath

class SaveDfsDk(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)

  // 이번 달 분석 결과 저장
  def saveMenuAnlysResDf(ethDt: String, menuAnlysResDf: DataFrame, flag: String) = {
    val filePathCls = new FilePath(ethDt: String, flag)
    fileFuncCls.wParquet(menuAnlysResDf, filePathCls.dkMenuAnlysResDfPath)
  }

  // 히스토리 저장
  def saveHstryDkbw(ethDt: String,
                    menuAnlysResDfBf1m: DataFrame,
                    hstryDkbBf1m :DataFrame,
                    hstryDkwBf1m: DataFrame,
                    flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val resDkbBf1m = menuAnlysResDfBf1m.filter('MENU_ID.startsWith("B"))
    val resDkwBf1m = menuAnlysResDfBf1m.filter('MENU_ID.startsWith("W"))

    val hstryDkb = hstryDkbBf1m.union(resDkbBf1m).distinct  // distinct 를 기존 코드 그대로 가져왔는데 없애도 되지 않나 싶음
    val hstryDkw = hstryDkwBf1m.union(resDkwBf1m).distinct  // distinct 를 기존 코드 그대로 가져왔는데 없애도 되지 않나 싶음

    fileFuncCls.wParquet(hstryDkb, filePathCls.dkHstryBeerPath)
    fileFuncCls.wParquet(hstryDkw, filePathCls.dkHstryWhiskyPath)
  }

  // WEB 저장
  def saveViewResWeb(ethDt: String, dkbWeb: DataFrame, dkwWeb: DataFrame, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val dkWeb = dkbWeb.union(dkwWeb)  // beer + whisky
    fileFuncCls.wParquet(dkWeb, filePathCls.dkViewResWebPath)
  }

  // 엑셀 저장
  def saveViewResExcel(ethDt: String, dkbExcel: DataFrame, dkwExcel: DataFrame, flag: String) = {
    val filePathCls = new FilePath(ethDt, flag)
    val dkExcel = dkbExcel.union(dkwExcel)  // beer + whisky
    fileFuncCls.wParquet(dkExcel, filePathCls.dkViewResExcelPath)
  }
}
