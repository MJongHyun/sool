/**

 */

package sool.address.refine_addr

import org.jsoup.Jsoup

class ScrapingAddr() extends java.io.Serializable {
  // 각 레벨별, 기준별 주소 추출
  def getAddrSeg(doc: org.jsoup.nodes.Document) = {
    // 건물관리번호 가져오기
    val bldngMgmtNmb1 = doc.select(".list_post>li").first().attr("data-building_code")
    val bldngMgmtNmb2 = doc.select(".txt_address.txt_sel.fst").first().attr("data-building_code")
    val bldngMgmtNmb3 = doc.select(".txt_address.txt_s.fst").first().attr("data-building_code")
    val bldngMgmtNmb = if (bldngMgmtNmb1 != "") {
      bldngMgmtNmb1
    } else if (bldngMgmtNmb2 != "") {
      bldngMgmtNmb2
    } else if (bldngMgmtNmb3 != "") {
      bldngMgmtNmb3
    } else {
      ""
    }
    bldngMgmtNmb
  }

  // 주소가 여러 개 나오면 pnu 코드를 체크하여 동일한 주소라고 봐도 될지 확인
  def checkPNU(doc: org.jsoup.nodes.Document) = {
    import scala.collection.JavaConverters._  // 여기에 있어야 아래 코드들에서 에러가 발생하지 않는다.

    val address_info = doc.select(".list_post li")  // 우편번호, 지번, 도로명를 포함하는 li 클래스
    val address_info2 = doc.select(".txt_address.txt_sel.fst")  // 도로명
    val address_info3 = doc.select(".txt_address.txt_s.fst")  // 지번
    val bldngMgmtNmbSet1 = address_info.iterator().asScala.toList.map(_.attr("data-building_code")).toSet
    val bldngMgmtNmbSet2 = address_info2.iterator().asScala.toList.map(_.attr("data-building_code")).toSet
    val bldngMgmtNmbSet3 = address_info3.iterator().asScala.toList.map(_.attr("data-building_code")).toSet
    val pnuSize1 = bldngMgmtNmbSet1.map(i => if (i != "") i.substring(0, 19)).size
    val pnuSize2 = bldngMgmtNmbSet2.map(i => if (i != "") i.substring(0, 19)).size
    val pnuSize3 = bldngMgmtNmbSet3.map(i => if (i != "") i.substring(0, 19)).size

    // PNU 가 동일한 경우만 건물관리번호를 구한다. 기존에는
    if ((pnuSize1 == 0 || pnuSize1 == 1) && (pnuSize2 == 0 || pnuSize2 == 1) && (pnuSize3 == 0 || pnuSize3 == 1)) {
      getAddrSeg(doc)
    } else {
      ""
    }
  }

  // 다음 주소 document 가져오기
  def getDoc(srcAdrs: String) = {
    val regex1 = """\([^)]*\)""".r
    val regex2 = """\d층""".r
    val cleanAddr1 = regex1.replaceAllIn(srcAdrs, "")
    val cleanAddr2 = regex2.replaceAllIn(cleanAddr1, "")
    val cleanAddr = cleanAddr2.trim() // DB 값이 trim 처리가 되어 저장되어 있기 때문에 trim 처리한다. 21.09.29 추가
    val doc = Jsoup.
      connect("https://spi.maps.daum.net/postcode/search").
      data("cpage1", "1").
      data("origin", "https,//spi.maps.daum.net").
      data("isp", "N").
      data("isgr", "N").
      data("isgj", "N").
      data("plrgt", "1.5").
      data("us", "on").
      data("msi", "10").
      data("ahs", "off").
      data("whas", "500").
      data("zn", "Y").
      data("sm", "on").
      data("CWinWidth", "400").
      data("fullpath", "/postcode/guidessl").
      data("a51", "off").
      data("region_name", cleanAddr).
      data("cq", cleanAddr).
      get()
    doc
  }

  // 다음 API 를 통해 건물관리번호, 검색결과개수 구하기
  def runScrapingAddr(srcAdrs: String): (String, Int) = {
    // 다음주소 document
    val doc = getDoc(srcAdrs)

    // 경고메세지 확인
    val alertDoc= doc.select(".emph_bold")
    val alert = if (!alertDoc.isEmpty) alertDoc.first().text() else ""

    // 검색결과 개수 확인
    val numPageDoc = doc.select(".num_page")
    val numPage = if (!numPageDoc.isEmpty) numPageDoc.first().attr("data-pagetotal").toInt else 0  // 페이지 수
    val numFirstPage = doc.select(".list_post li").size // 첫번째 장에 나온 결과 수
    val srchRsltsCnt = numPage * numFirstPage // 합

    // 검색결과가 없거나 너무 많은 경우는 제외, 검색결과가 1개 이상이면 PNU코드 체그
    // 유사한 검색 결과라고 뜨는 것도 수집하도록 되어 있다.
    val bldngMgmtNmb = {
      if (srchRsltsCnt == 0 || alert == "검색결과가 없습니다." || alert == "검색결과가 많습니다.") {
        ""
      } else if (srchRsltsCnt == 1) {
        getAddrSeg(doc)
      } else {
        checkPNU(doc)
      }
    }
    (bldngMgmtNmb, srchRsltsCnt)
  }
}