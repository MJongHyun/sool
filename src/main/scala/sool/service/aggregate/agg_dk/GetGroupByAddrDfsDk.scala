/**

 */
package sool.service.aggregate.agg_dk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct, lit, sum}

class GetGroupByAddrDfsDk(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  // groupBy Cols
  def getGroupByColsMap() = {
    val groupByColsMap = Map(
      "dnAddr0G" -> Seq("MENU_ID"),
      "dnAddr1G" -> Seq("MENU_ID", "ADDR1"),
      "dnAddr2G" -> Seq("MENU_ID", "ADDR1", "ADDR2"),
      "dnAddr3G" -> Seq("MENU_ID", "ADDR1", "ADDR2", "ADDR3")
    ).transform((_, v) => v.map(col))
    groupByColsMap
  }

  // aggregate Cols
  def getAggCols(option: String) = {
    val aggCols = if (option == "DENO") {
      Seq(
        sum("QT").as("QT_MARKET"),
        sum("ITEM_QT").as("BOTTLE_MARKET"),
        sum("SUP_AMT").as("AMT_MARKET"),
        countDistinct("BYR_RGNO").as("DEAL_MARKET")
      )
    } else if (option == "NUME") {
      Seq(
        sum("QT").as("QT_MARKET_DK"),
        sum("ITEM_QT").as("BOTTLE_MARKET_DK"),
        sum("SUP_AMT").as("AMT_MARKET_DK"),
        countDistinct("BYR_RGNO").as("DEAL_MARKET_DK")
      )
    } else {
      Seq("").map(col) // option 잘못 입력했을 때 일부러 에러 발생시키려고 else 문을 따로 넣음
    }
    aggCols
  }

  // 컬럼 추가 리스트
  def getSlctAddColsMap() = {
    val slctAddColsMap = Map(
      "dnAddr0G" -> Seq(
        lit("전국").as("ADDR1"), lit("전체").as("ADDR2"),
        lit("전체").as("ADDR3"), lit(0).as("ADDR_LVL")
      ),
      "dnAddr1G" -> Seq(
        lit("전체").as("ADDR2"), lit("전체").as("ADDR3"),
        lit(0).as("ADDR_LVL")
      ),
      "dnAddr2G" -> Seq(
        lit("전체").as("ADDR3"), lit(1).as("ADDR_LVL")
      ),
      "dnAddr3G" -> Seq(
        lit(2).as("ADDR_LVL")
      )
    )
    slctAddColsMap
  }

  // 컬럼 순서
  def getSlctCols(option: String) = {
    val slctCols = if (option == "DENO") {
      Seq(
        "MENU_ID", "ADDR1", "ADDR2", "ADDR3", "QT_MARKET",
        "BOTTLE_MARKET", "AMT_MARKET", "DEAL_MARKET", "ADDR_LVL"
      ).map(col)
    } else if (option == "NUME") {
      Seq(
        "MENU_ID", "ADDR1", "ADDR2", "ADDR3", "QT_MARKET_DK",
        "BOTTLE_MARKET_DK", "AMT_MARKET_DK", "DEAL_MARKET_DK", "ADDR_LVL"
      ).map(col)
    } else {
      Seq("").map(col)
    }
    slctCols
  }

  // 분모 마트를 MENU_ID 및 주소 컬럼들로 그룹화하여 값들을 구하는 함수
  def getDnAddrG(martDkDeno: DataFrame, option: String) = {
    val groupByColsMap = getGroupByColsMap()
    val aggCols = getAggCols(option)
    val slctAddColsMap = getSlctAddColsMap()
    val slctCols = getSlctCols(option)

    val dnAddr0G = martDkDeno.groupBy(groupByColsMap("dnAddr0G"):_*).agg(aggCols.head, aggCols.tail:_*)
    val dnAddr0AddCols = dnAddr0G.select($"*" +: slctAddColsMap("dnAddr0G"):_*)
    val dnAddr0 = dnAddr0AddCols.select(slctCols:_*)

    val dnAddr1G = martDkDeno.groupBy(groupByColsMap("dnAddr1G"):_*).agg(aggCols.head, aggCols.tail:_*)
    val dnAddr1AddCols = dnAddr1G.select($"*" +: slctAddColsMap("dnAddr1G"):_*)
    val dnAddr1 = dnAddr1AddCols.select(slctCols:_*)

    val dnAddr2G = martDkDeno.groupBy(groupByColsMap("dnAddr2G"):_*).agg(aggCols.head, aggCols.tail:_*)
    val dnAddr2AddCols = dnAddr2G.select($"*" +: slctAddColsMap("dnAddr2G"):_*)
    val dnAddr2 = dnAddr2AddCols.select(slctCols:_*)

    val dnAddr3G = martDkDeno.groupBy(groupByColsMap("dnAddr3G"):_*).agg(aggCols.head, aggCols.tail:_*)
    val dnAddr3AddCols = dnAddr3G.select($"*" +: slctAddColsMap("dnAddr3G"):_*)
    val dnAddr3 = dnAddr3AddCols.select(slctCols:_*)

    val dnAddrG = dnAddr0.union(dnAddr1).union(dnAddr2).union(dnAddr3)
    dnAddrG
  }

  // 분자 데이터에서 에러 리스트 제거 및 더블 카운팅 문제 해결
  def getMartDkNumeAddrGExceptMnId(martDkNumeAddrG: DataFrame,
                                   exceptMnIdList: Seq[(String, String, String)],
                                   exceptMnIdDf: DataFrame) = {
    // Type1: 에러 리스트를 제외한 정답 결과 셋
    val martDkNumeAddrGType1 = martDkNumeAddrG.
      join(exceptMnIdDf, Seq("MENU_ID"), "left").
      filter('EXIST.isNull).
      drop("EXIST")

    // Type2: 홉하우스 + 기네스 업체 수 더블 카운팅 문제 데이터 결과 셋
    val martDkNumeAddrGType2Pre1 = exceptMnIdList.map(menuId => {
      val resId = menuId._1
      val guinnessResId = menuId._2
      val hophouseResId = menuId._3

      val gOrhFltr = martDkNumeAddrG.filter('MENU_ID === guinnessResId || 'MENU_ID === hophouseResId)
      val gOrhFltrDrop = gOrhFltr.drop("MENU_ID")
      val gOrhFltrDropAddCol = gOrhFltrDrop.withColumn("MENU_ID", lit(resId))
      gOrhFltrDropAddCol
    }).reduce(_ union _)

    val martDkNumeAddrGType2Pre2 = martDkNumeAddrGType2Pre1.
      groupBy("MENU_ID", "ADDR_LVL", "ADDR1", "ADDR2", "ADDR3").
      agg(
        sum("QT_MARKET_DK").as("QT_MARKET_DK"),
        sum("AMT_MARKET_DK").as("AMT_MARKET_DK"),
        sum("BOTTLE_MARKET_DK").as("BOTTLE_MARKET_DK"),
        sum("DEAL_MARKET_DK").as("DEAL_MARKET_DK")
      )

    val martDkNumeAddrGType2 = martDkNumeAddrGType2Pre2.
      select(
        'MENU_ID, 'ADDR1, 'ADDR2,
        'ADDR3, 'QT_MARKET_DK, 'BOTTLE_MARKET_DK,
        'AMT_MARKET_DK, 'DEAL_MARKET_DK, 'ADDR_LVL
      )

    // 최종 결과
    val martDkNumeAddrGExceptMnId = martDkNumeAddrGType1.union(martDkNumeAddrGType2)
    martDkNumeAddrGExceptMnId
  }

  // 메인
  def getGroupByAddrDfsDk(martDkbDeno: DataFrame,
                          martDkwDeno: DataFrame,
                          martDkbNume: DataFrame,
                          martDkwNume: DataFrame) = {
    // 분모
    val martDkbDenoAddrG = getDnAddrG(martDkbDeno, "DENO")
    val martDkwDenoAddrG = getDnAddrG(martDkwDeno, "DENO")
    val martDkDenoAddrG = martDkbDenoAddrG.union(martDkwDenoAddrG)

    // 분자
    val martDkbNumeAddrG = getDnAddrG(martDkbNume, "NUME")
    val martDkwNumeAddrG = getDnAddrG(martDkwNume, "NUME")
    val martDkNumeAddrG = martDkbNumeAddrG.union(martDkwNumeAddrG)
    (martDkDenoAddrG, martDkNumeAddrG)
  }
}
