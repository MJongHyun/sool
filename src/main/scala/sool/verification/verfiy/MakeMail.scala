package sool.verification.verfiy

import sool.common.function.{FileFunc, GetTime}
import sool.common.jdbc.JdbcGet
import sool.common.path.FilePath

class MakeMail(spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  // 필요 클래스 선언
  val fileFuncCls = new FileFunc(spark)
  val getTimeCls = new GetTime()
  val jdbcGetCls = new JdbcGet(spark)

  def make_percent(mlclr : Long, dnmnt : Long) = {
    val per = BigDecimal((mlclr.toDouble/dnmnt.toDouble)*100).setScale(1, BigDecimal.RoundingMode.HALF_UP)
    per+"%"
  }

  def wXlsx_custom(df:org.apache.spark.sql.DataFrame, path:String, sheet_num:String) = {
    if (sheet_num=="one")
      df.write.
        format("com.crealytics.spark.excel").
        option("dataAddress", s"'Sheet1'!B3:G14").
        option("header", "true").
        mode("append").
        save(s"${path}")
    else if (sheet_num=="two"){
      df.write.
        format("com.crealytics.spark.excel").
        option("dataAddress", s"'Sheet1'!J3:L20").
        option("header", "true").
        mode("append").
        save(s"${path}")
    }
    else if (sheet_num=="three"){
      df.write.
        format("com.crealytics.spark.excel").
        option("dataAddress", s"'Sheet1'!O3:Z20").
        option("header", "true").
        mode("append").
        save(s"${path}")
    }
  }

  def mkComProccess(ethDt: String, flag: String) ={
    val filePathCls = new FilePath(ethDt, flag)
    val ComMaster = fileFuncCls.rParquet(filePathCls.comMainPath)
    // ethDT
    val new_cDT =  ComMaster.filter($"ADDR_TYPE" === "NEW").filter($"ADDR_DATE" === ethDt).count()
    val new_bfDT =  ComMaster.filter($"ADDR_TYPE" === "NEW").filter($"ADDR_DATE" =!= ethDt).count()
    val new_total = ComMaster.filter($"ADDR_TYPE" === "NEW").count()

    val old_cDT =  ComMaster.filter($"ADDR_TYPE" === "OLD").filter($"ADDR_DATE" === ethDt).count()
    val old_bfDT =  ComMaster.filter($"ADDR_TYPE" === "OLD").filter($"ADDR_DATE" =!= ethDt).count()
    val old_total = ComMaster.filter($"ADDR_TYPE" === "OLD").count()

    val mystery = ComMaster.filter($"ADDR_TYPE" === "BF1M").count()
    val not_mat_add = ComMaster.filter($"ADDR_TYPE".isNull).count()

    val all_total = ComMaster.count()
    val mat_total = all_total-not_mat_add
    val fix = ComMaster.filter($"ADDR_TYPE" === "FIX").count()

    val com_process = Seq(
      ("정제","신규 프로세스 주소","정제",new_cDT, make_percent(new_total, all_total), make_percent(mat_total, all_total)),
      ("정제","신규 프로세스 주소","과거 주소데이터 활용",new_bfDT, make_percent(new_total, all_total),make_percent(mat_total, all_total)),
      ("정제","기존 프로세스 주소","정제",old_cDT, make_percent(old_total, all_total), make_percent(mat_total, all_total)),
      ("정제","기존 프로세스 주소","과거 주소데이터 활용",old_bfDT, make_percent(old_total, all_total), make_percent(mat_total, all_total)),
      ("정제","출처를 알 수 없는 과거에 정제된 주소","출처를 알 수 없는 과거에 정제된 주소",mystery, make_percent(mystery, all_total), make_percent(mat_total, all_total)),
      ("정제","고정주소","고정주소",fix, make_percent(fix, all_total), make_percent(mat_total, all_total)),
      ("정제","총 정제된 업체 수","총 정제된 업체 수",mat_total, make_percent(mat_total, all_total), make_percent(mat_total, all_total)),
      ("비정제","-","-",not_mat_add, make_percent(not_mat_add, all_total), make_percent(not_mat_add, all_total)),
      ("총 업체수","-","-",all_total, make_percent(all_total, all_total), make_percent(all_total, all_total))
    ).toDF("분류", "분류","분류","업체 수","세부 백분율","정제 유/무 백분율")
    wXlsx_custom(com_process, filePathCls.maikDataPath, "one")
  }

  def itemCheck(ethDt: String, flag: String)={
    val filePathCls = new FilePath(ethDt, flag)
    val item_info = fileFuncCls.rXlsx(filePathCls.itemMatRstPath)
    val all_verify_cnt = item_info.filter($"SAW"==="안봄").count()
    val human_verify  = item_info.filter($"SAW"==="안봄").filter($"CATEGORY_IDX" ==="1" || $"CATEGORY_IDX" ==="2" || $"CATEGORY_IDX" ==="6").filter($"predict_label"==="1").count()
    val auto_verify = all_verify_cnt-human_verify
    val itme_ver_process = Seq(
      ("자동검증",auto_verify, make_percent(auto_verify,all_verify_cnt)),
      ("수동검증",human_verify, make_percent(human_verify,all_verify_cnt)),
      ("합계",all_verify_cnt, make_percent(all_verify_cnt,all_verify_cnt))
    ).toDF("아이템 검증","아이템 수","백분율")
    wXlsx_custom(itme_ver_process, filePathCls.maikDataPath, "two")
  }

  def itemMap(ethDt: String, flag: String)={
    val ethBf1m = getTimeCls.getEthBf1m(ethDt)   // 집계연월의 전 달
    val filePathClsBf1m = new FilePath(ethBf1m, flag)
    val filePathCls = new FilePath(ethDt, flag)
    // 전달/이번달 누적사전 검증 갯수 확인
    val cumulBF2 = spark.read.parquet(filePathClsBf1m.cumulNminfoDfNewPath)
    val cumulAF = spark.read.parquet(filePathCls.cumulNminfoDfPath)
    val bf2_check_item_cnt = cumulBF2.filter($"SAW"=!="안봄").count()
    val af_check_item_cnt = cumulAF.filter($"SAW"=!="안봄").count()
    val current_cnt = cumulAF.count
    val item_mapping = Seq(
      ( ethBf1m.toString, bf2_check_item_cnt,make_percent(bf2_check_item_cnt,current_cnt)),
      ( ethDt.toString, af_check_item_cnt, make_percent(af_check_item_cnt,current_cnt)),
      ("현재 총 아이템 수",current_cnt, make_percent(current_cnt,current_cnt))
    ).toDF("검증완료날짜","누적아이템 수","총 아이템 수 대비")
    wXlsx_custom(item_mapping, filePathCls.maikDataPath, "three")
  }

  // 메인
  def runMkMail(ethDt: String, flag: String) = {
    mkComProccess(ethDt, flag)
    itemCheck(ethDt, flag)
    itemMap(ethDt, flag)
  }
}
