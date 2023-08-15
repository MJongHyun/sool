/**
 * file path

 */
package sool.common.path

class FilePath (ethDt: String, flag: String) extends java.io.Serializable {
  // s3 루트 경로
  var s3RootPath = "s3a://sool/"
  var rootPath = s3RootPath
  var hjPath = "HJ/"
  var hj2Path = "HJ2/"
  var dkPath = "DK/"
  var hkPath = "HK/"
  var soolMasterPath = "Master/"
  var soolResultPath = "Result/"
  var soolBasePath = "Base/"
  var soolInvoicePath = ""
  var soolCommonPath = "COMMON/"
  var invDataPath = ""
  // sool/COMMON/Address
  var soolCommonAddressPath = rootPath + soolCommonPath + "ADDRESS/"
//  var fixAddrPath = soolCommonAddressPath + "fixAddr.parquet"

  // 공통
  var addressPath = rootPath + soolBasePath + ethDt + "/Address/"
  var dataPath = rootPath + soolBasePath + ethDt + "/Data/"
  var dataEnterprisePath = dataPath + "Enterprise/"
  var baseCommonPath = rootPath + soolBasePath + "common/"
  var modelPath = rootPath + soolBasePath + ethDt + "/Model/"
  var excelPath = rootPath + soolBasePath + ethDt + "/Excel/"
  var verifyPath = rootPath + soolBasePath + ethDt + "/Verify/"

  // s3a://sool/Base/common/ 관련 파일 경로
  var baseCmmnNonEthComPath = baseCommonPath + "NonEthanolCom.parquet"
  var nonCmmnEthItemPath = baseCommonPath + "NonEthanolComItem.parquet"

  // s3a://sool/Base/집계연월/Data/ 관련 파일 경로
  var dtiPath = dataPath + "dti.parquet"
  var dtiEthPath = dataPath + "dtiEthanol.parquet"
  var dtiCh3Path = dataPath + "dtiChannel3.parquet"
  var dtiRtlPath = dataPath + "dtiRetail.parquet"
  var nonEthComPath = dataPath + "NonEthanolCom.parquet"
  var basePath = dataPath + "base.parquet"
  var baseTaggedPath = dataPath + "base_tagged.parquet"
  var comMainPrePath = dataPath + "com_main_pre.parquet"
  var nonEthItemPath = dataPath + "NonEthanolComItem.parquet"

  // s3a://sool/Base/집계연월/Data/Enterprise/ 관련 파일 경로
  var vendorPath = dataEnterprisePath + "vendor.parquet"
  var retailPath = dataEnterprisePath + "retail.parquet"
  var newComPath = dataEnterprisePath + "newCom.parquet"

  // s3a://sool/Base/집계연월/Address/ 관련 파일 경로
  var rawAddrPath = addressPath + s"rawAddr_${ethDt}.csv"
  var bldngMgmtNmbAddrPath = addressPath + s"bldngMgmtNmbAddr_${ethDt}.parquet"
  var moAddrPath = addressPath + s"MoAddr_${ethDt}.csv"
  var addrMatchingPath = addressPath + s"addrMatching_${ethDt}.parquet"
  var bfComMasterPath = addressPath + "bfComMaster.csv"
  var comMainCsvPath = addressPath + "com_main.csv"
  var comMainPath = addressPath + "com_main.parquet"
  var comMainDkPath = addressPath + "com_main_dk.parquet"

  // s3a://sool/Base/집계연월/Model/ 관련 파일 경로
  var cumulNminfoDfPath = modelPath + "cumul_nminfo_df.parquet"
  var cumulNminfoDfNewPath = modelPath + "NewCumul/" + "cumul_nminfo_df.parquet"
  var existedDfPath = modelPath + "existedDF.parquet"
  var generatedDfPath = modelPath + "generatedDF.parquet"
  var beforeDictDfPath = modelPath + "beforeDictDF.parquet"
  var taggedDfPath = modelPath + "taggedDF.parquet"
  var itemInfoBfPath = modelPath + "item_info_BEFORE.parquet"
  var itemInfoAfPath = modelPath + "item_info_AFTER.parquet"
  var itemInfoAfXlxPath = modelPath + "item_info_AFTER.xlsx"
  var itemInfoFnlPath = modelPath + "item_info_FINAL.parquet"
  var fnlNminfoDfPath = modelPath + "final_nminfo_df.parquet"
  var itemMatRstPath = modelPath + "item_matching_result.xlsx"

  // s3a://sool/Base/집계연월/Excel/ 관련 파일 경로
  var itemInfoAfExcelPath = modelPath + "item_info_AFTER.xlsx"
  var itemInfoFnlCsvPath = modelPath + "item_info_FINAL.csv"

  // HJ 1
  var hjMasterPath = rootPath + hjPath + soolMasterPath
  var hjResultPath = rootPath + hjPath + soolResultPath + ethDt + "/"
  var hjCdDimensionPath = hjMasterPath + "CD_DIMENSION.parquet"
  var hjrMartBPath = hjResultPath + "mart_hite_beer.parquet/*"
  var hjrMartSPath = hjResultPath + "mart_hite_soju.parquet/*"
  var hjwMartBPath = hjResultPath + "mart_hite_beer.parquet"
  var hjwMartSPath = hjResultPath + "mart_hite_soju.parquet"
  var hjFirstTradeListPath = hjMasterPath + "FIRST_TRADE_LIST1.parquet"
  var hjSaUpJangPath = hjMasterPath + "SA_UP_JANG.parquet"
  var hjwMartBExceptAddrPath = hjResultPath + "mart_hite_non_addr_beer.parquet"
  var hjwMartSExceptAddrPath = hjResultPath + "mart_hite_non_addr_soju.parquet"
  var hjwLv2Path = hjMasterPath + "LV2W_MAP.parquet"
  var hjSupCdDimensionPath = hjMasterPath + "SUP_CD_DIMENSION.parquet"
  var hjAnvrSavePath = hjResultPath + "aggResult/parquet/"
  var hjAnvr01Path = hjAnvrSavePath + "ANVR01.parquet"
  var hjAnvr02Path = hjAnvrSavePath + "ANVR02.parquet"
  var hjAnvr03Path = hjAnvrSavePath + "ANVR03.parquet"
  var hjAnvr04Path = hjAnvrSavePath + "ANVR04.parquet"
  var hjAnvr05Path = hjAnvrSavePath + "ANVR05.parquet"
  var hjAnvr06Path = hjAnvrSavePath + "ANVR06.parquet"
  var hjTsvSavePath = hjResultPath + "aggResult/hite" + ethDt + "/"
  var hjCdAnvrParquetPath = hjMasterPath + "CD_ANVR.parquet"
  var hjSupMartBPath = hjResultPath + "supMart_hite_beer.parquet"
  var hjSupMartSPath = hjResultPath + "supMart_hite_soju.parquet"
  var hjSupDenoNumePath = hjResultPath + "aggResult/supParquet/hjSupDenoNume.parquet"
  var hjAnvr13Path = hjTsvSavePath + "ANVR13.tsv"
  var hjMartPath = hjResultPath + "mart_hite.parquet"

  // HJ 2
  var hj2MasterPath = rootPath + hj2Path + soolMasterPath
  var hj2DataPath = rootPath + hj2Path + soolResultPath + ethDt + "/Data/"
  var hj2FilePath = rootPath + hj2Path + soolResultPath + ethDt + "/File/"
  var hj2TerritoryOrderPath = hj2MasterPath + "territory_order.parquet"
  var hj2ComRgnoPath = hj2DataPath + "com.parquet"
  var hj2AnlysMartPath = hj2DataPath + "mart_hite2.parquet"
  var hj2ComResPath = hj2DataPath + "COMP_INFO.parquet"
  var hj2AnlysResPath = hj2DataPath + "ANALYSIS_RESULT.parquet"
  var hj2ComResTsvPath = hj2FilePath + "COMP_INFO.tsv"
  var hj2AnlysResTsvPath = hj2FilePath + "ANALIYSIS_RESULT.tsv"

  // HJ 3
  var hj3ParquetPath = s3RootPath + "HJ3/" + ethDt + "/parquet/"
  var hj3XlsxPath = s3RootPath + "HJ3/" + ethDt + "/xlsx/"
  var hj3BusanParquetPath = hj3ParquetPath + "BusanSojuMs.parquet"
  var hj3BusanXlsxPath = hj3XlsxPath + "BusanSojuMs.xlsx"
  var hj3SudoGwonParquetPath =  hj3ParquetPath + "sudoBeerMs.parquet"
  var hj3SudoGwonXlsxPath = hj3XlsxPath + "sudoBeerMs.xlsx"


  // DK
  var dkMasterPath = rootPath + dkPath + soolMasterPath
  var dkResultPath = rootPath + dkPath + soolResultPath + ethDt + "/"
  var dkWholeSalePath = dkMasterPath + "WHOLE_SALE.parquet"
  var dkwMartBPath = dkResultPath + "mart_dk_beer.parquet"
  var dkwMartWPath = dkResultPath + "mart_dk_whisky.parquet"
  var comMasterDkPath = addressPath + "com_main_dk.parquet"
  var dkMenuAnlysResDfPath = dkResultPath + "menuAnalysisResultDF.parquet"
  var dkHstryBeerPath = dkResultPath + "history_Beer.parquet"
  var dkHstryWhiskyPath = dkResultPath + "history_Whisky.parquet"
  var dkViewResExcelBeerPath = dkResultPath + s"viewResult_EXCEL_${ethDt}_Beer.parquet"
  var dkViewResExcelWhiskyPath = dkResultPath + s"viewResult_EXCEL_${ethDt}_Whisky.parquet"
  var dkViewResExcelPath = dkResultPath + s"viewResult_EXCEL_${ethDt}.parquet"
  var dkViewResWebBeerPath = dkResultPath + s"viewResult_WEB_${ethDt}_Beer.parquet"
  var dkViewResWebWhiskyPath = dkResultPath + s"viewResult_WEB_${ethDt}_Whisky.parquet"
  var dkViewResWebPath = dkResultPath + s"viewResult_WEB_${ethDt}.parquet"

  // HK
  var hkMasterPath = rootPath + hkPath + soolMasterPath
  var hkBranchDonePath = hkMasterPath + "branch_Done.parquet"
  var hkResultPath = rootPath + hkPath + soolResultPath + ethDt + "/"
  var hkrMartPath = hkResultPath + "martHK.parquet/*"
  var hkwMartPath = hkResultPath + "martHK.parquet"
  var hkBranchPath = hkResultPath + "branch_Done.parquet"
  var hkDenoPath = hkResultPath + "denoDF.parquet"
  var hkNumePath = hkResultPath + "numeDF.parquet"
  var hkResPath = hkResultPath + "resultHK.parquet"
  var hkMrtPath = hkResultPath + s"Heineken_${ethDt}.xlsx"

  // master update
  var itemMasterPath = rootPath + soolCommonPath + "ITEMMASTER/"
  var afItemPath = itemMasterPath + "main.xlsx"
  var ItemPath = rootPath + soolBasePath + ethDt + "/Model/final_main_df.parquet"
  // HJ master path
  var hjBfPath = rootPath
  // DK master path
  var bfDkbPath = dkMasterPath + "DKB_ITEM_MASTER.parquet"
  var afDkbPath = itemMasterPath + "DKB_ITEM_MASTER.xlsx"
  var bfDkwPath = dkMasterPath + "DKW_ITEM_MASTER.parquet"
  var afDkwPath = itemMasterPath + "DKW_ITEM_MASTER.xlsx"
  var dkMenuBfPath = dkMasterPath + "DK_MENU_ANALYSIS.parquet"
  var dkMenuAfPath = itemMasterPath + "DK_MENU_ANALYSIS.xlsx"
  var dkMenuhistPath = itemMasterPath + "BEFORE/" + ethDt + "/DK_MENU_ANALYSIS.parquet"
  var dkbHistPath = itemMasterPath + "BEFORE/" + ethDt + "/DKB_ITEM_MASTER.parquet"
  var dkwHistPath = itemMasterPath + "BEFORE/" + ethDt + "/DKW_ITEM_MASTER.parquet"
  // HJ
  var bfHjsPath = hjMasterPath + "HJS_ITEM_MASTER.parquet"
  var afHjsPath = itemMasterPath + "HJS_ITEM_MASTER.xlsx"
  var bfHjbPath = hjMasterPath + "HJB_ITEM_MASTER.parquet"
  var afHjbPath = itemMasterPath + "HJB_ITEM_MASTER.xlsx"
  var hjCodeBfPath = hjMasterPath + "CD_DIMENSION.parquet"
  var hjCodeAfPath = itemMasterPath + "CD_DIMENSION.xlsx"
  var hj2ItemBfPath = hjMasterPath + "HJ2_ITEM.parquet"
  var hj2ItemAfPath = itemMasterPath + "HJ2_ITEM.xlsx"
  var hjsHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJS_ITEM_MASTER.parquet"
  var hjbHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJB_ITEM_MASTER.parquet"
  var hjCodeHistPath = itemMasterPath + "BEFORE/" + ethDt + "/CD_DIMENSION.parquet"
  var hj2ItemHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJ2_ITEM.parquet"
  // HK
  var bfHkCodePath = hkMasterPath + "main.xlsx"
  var afHkCodePath = itemMasterPath + "main_hk.xlsx"
  var hkHistPath = itemMasterPath + "BEFORE/" + ethDt + "/main_hk.parquet"
  // AngelNet
  var angelItemPath = s3RootPath + "ANGELNET/ItemMaster/DKItemMaster_DictSize.parquet"
  var angelItemHistPath = s3RootPath + "ANGELNET/" + ethDt + "/ItemMaster/DKItemMaster_DictSize.parquet"
  //결과 검증 데이터 결과 저장 경로
  var dataCollectPath = rootPath + soolBasePath + ethDt + "/Verify/데이터수집량_공급업체이슈_" + ethDt + ".xlsx"
  var dataCollectIssuePath = rootPath + soolBasePath + ethDt + "/Verify/data_clctn_issue_" + ethDt + ".parquet"
  var dataCollectSupPath = rootPath + soolBasePath + ethDt + "/Verify/new_sup_issue_" + ethDt + ".parquet"
  var missingSupPath = rootPath + soolBasePath + ethDt + "/Verify/missing_sup_issue_" + ethDt + ".parquet"
  var verifyTimePath = verifyPath + s"시계열추이_${ethDt}.xlsx"
  var verifyMartPath = verifyPath + s"데이터마트_검증파일_${ethDt}.xlsx"
  var maikDataPath = verifyPath + "mailData.xlsx"
  var hkVerifyPath = hkResultPath + "Heineken_verify_" + ethDt + ".xlsx"

  var invCumulNminfoDfPath = ""
  var rootDtiPath = ""
  var dtiCumulNminfoDfPath = ""
  var dtiCumulNminfoDfNewPath = ""

  if (flag=="dti") {
    // s3 루트 경로
    s3RootPath = "s3a://sool/"
    rootPath = s3RootPath

    hjPath = "HJ/"
    hj2Path = "HJ2/"
    dkPath = "DK/"
    hkPath = "HK/"
    soolMasterPath = "Master/"
    soolResultPath = "Result/"
    soolBasePath = "Base/"
    soolCommonPath = "COMMON/"

    // sool/COMMON/Address
    soolCommonAddressPath = rootPath + soolCommonPath + "ADDRESS/"
//    fixAddrPath = soolCommonAddressPath + "fixAddr.parquet"

    // 공통
    addressPath = rootPath + soolBasePath + ethDt + "/Address/"
    dataPath = rootPath + soolBasePath + ethDt + "/Data/"
    dataEnterprisePath = dataPath + "Enterprise/"
    baseCommonPath = rootPath + soolBasePath + "common/"
    modelPath = rootPath + soolBasePath + ethDt + "/Model/"
    excelPath = rootPath + soolBasePath + ethDt + "/Excel/"
    verifyPath = rootPath + soolBasePath + ethDt + "/Verify/"

    // s3a://sool/Base/common/ 관련 파일 경로
    baseCmmnNonEthComPath = baseCommonPath + "NonEthanolCom.parquet"
    nonCmmnEthItemPath = baseCommonPath + "NonEthanolComItem.parquet"

    // s3a://sool/Base/집계연월/Data/ 관련 파일 경로
    dtiPath = dataPath + "dti.parquet"
    dtiEthPath = dataPath + "dtiEthanol.parquet"
    dtiCh3Path = dataPath + "dtiChannel3.parquet"
    dtiRtlPath = dataPath + "dtiRetail.parquet"
    nonEthComPath = dataPath + "NonEthanolCom.parquet"
    basePath = dataPath + "base.parquet"
    baseTaggedPath = dataPath + "base_tagged.parquet"
    comMainPrePath = dataPath + "com_main_pre.parquet"
    nonEthItemPath = dataPath + "NonEthanolComItem.parquet"

    // s3a://sool/Base/집계연월/Data/Enterprise/ 관련 파일 경로
    vendorPath = dataEnterprisePath + "vendor.parquet"
    retailPath = dataEnterprisePath + "retail.parquet"
    newComPath = dataEnterprisePath + "newCom.parquet"

    // s3a://sool/Base/집계연월/Address/ 관련 파일 경로
    rawAddrPath = addressPath + s"rawAddr_${ethDt}.csv"
    bldngMgmtNmbAddrPath = addressPath + s"bldngMgmtNmbAddr_${ethDt}.parquet"
    moAddrPath = addressPath + s"MoAddr_${ethDt}.csv"
    addrMatchingPath = addressPath + s"addrMatching_${ethDt}.parquet"
    bfComMasterPath = addressPath + "bfComMaster.csv"
    comMainCsvPath = addressPath + "com_main.csv"
    comMainPath = addressPath + "com_main.parquet"
    comMainDkPath = addressPath + "com_main_dk.parquet"

    // s3a://sool/Base/집계연월/Model/ 관련 파일 경로
    cumulNminfoDfPath = modelPath + "cumul_nminfo_df.parquet"
    cumulNminfoDfNewPath = modelPath + "NewCumul/" + "cumul_nminfo_df.parquet"
    existedDfPath = modelPath + "existedDF.parquet"
    generatedDfPath = modelPath + "generatedDF.parquet"
    beforeDictDfPath = modelPath + "beforeDictDF.parquet"
    taggedDfPath = modelPath + "taggedDF.parquet"
    itemInfoBfPath = modelPath + "item_info_BEFORE.parquet"
    itemInfoAfPath = modelPath + "item_info_AFTER.parquet"
    itemInfoAfXlxPath = modelPath + "item_info_AFTER.xlsx"
    itemInfoFnlPath = modelPath + "item_info_FINAL.parquet"
    fnlNminfoDfPath = modelPath + "final_nminfo_df.parquet"
    itemInfoAfExcelPath = modelPath + "item_info_AFTER.xlsx"
    itemInfoFnlCsvPath = modelPath + "item_info_FINAL.csv"
    itemMatRstPath = modelPath + "item_matching_result.xlsx"

    // HJ 1
    hjMasterPath = rootPath + hjPath + soolMasterPath
    hjResultPath = rootPath + hjPath + soolResultPath + ethDt + "/"
    hjCdDimensionPath = hjMasterPath + "CD_DIMENSION.parquet"
    hjrMartBPath = hjResultPath + "mart_hite_beer.parquet/*"
    hjrMartSPath = hjResultPath + "mart_hite_soju.parquet/*"
    hjwMartBPath = hjResultPath + "mart_hite_beer.parquet"
    hjwMartSPath = hjResultPath + "mart_hite_soju.parquet"
    hjFirstTradeListPath = hjMasterPath + "FIRST_TRADE_LIST1.parquet"
    hjSaUpJangPath = hjMasterPath + "SA_UP_JANG.parquet"
    hjwMartBExceptAddrPath = hjResultPath + "mart_hite_non_addr_beer.parquet"
    hjwMartSExceptAddrPath = hjResultPath + "mart_hite_non_addr_soju.parquet"
    hjwLv2Path = hjMasterPath + "LV2W_MAP.parquet"
    hjSupCdDimensionPath = hjMasterPath + "SUP_CD_DIMENSION.parquet"
    hjAnvrSavePath = hjResultPath + "aggResult/parquet/"
    hjAnvr01Path = hjAnvrSavePath + "ANVR01.parquet"
    hjAnvr02Path = hjAnvrSavePath + "ANVR02.parquet"
    hjAnvr03Path = hjAnvrSavePath + "ANVR03.parquet"
    hjAnvr04Path = hjAnvrSavePath + "ANVR04.parquet"
    hjAnvr05Path = hjAnvrSavePath + "ANVR05.parquet"
    hjAnvr06Path = hjAnvrSavePath + "ANVR06.parquet"
    hjTsvSavePath = hjResultPath + "aggResult/hite" + ethDt + "/"
    hjCdAnvrParquetPath = hjMasterPath + "CD_ANVR.parquet"
    hjSupMartBPath = hjResultPath + "supMart_hite_beer.parquet"
    hjSupMartSPath = hjResultPath + "supMart_hite_soju.parquet"
    hjSupDenoNumePath = hjResultPath + "aggResult/supParquet/hjSupDenoNume.parquet"
    hjAnvr13Path = hjTsvSavePath + "ANVR13.tsv"
    hjMartPath = hjResultPath + "mart_hite.parquet"

    // HJ 2
    hj2MasterPath = rootPath + hj2Path + soolMasterPath
    hj2DataPath = rootPath + hj2Path + soolResultPath + ethDt + "/Data/"
    hj2FilePath = rootPath + hj2Path + soolResultPath + ethDt + "/File/"
    hj2TerritoryOrderPath = hj2MasterPath + "territory_order.parquet"
    hj2ComRgnoPath = hj2DataPath + "com.parquet"
    hj2AnlysMartPath = hj2DataPath + "mart_hite2.parquet"
    hj2ComResPath = hj2DataPath + "COMP_INFO.parquet"
    hj2AnlysResPath = hj2DataPath + "ANALYSIS_RESULT.parquet"
    hj2ComResTsvPath = hj2FilePath + "COMP_INFO.tsv"
    hj2AnlysResTsvPath = hj2FilePath + "ANALIYSIS_RESULT.tsv"

    // HJ 3
    // HJ 3
    hj3ParquetPath = s3RootPath + "HJ3/" + ethDt + "/parquet/"
    hj3XlsxPath = s3RootPath + "HJ3/" + ethDt + "/xlsx/"
    hj3BusanParquetPath = hj3ParquetPath + "BusanSojuMs.parquet"
    hj3BusanXlsxPath = hj3XlsxPath + "BusanSojuMs.xlsx"
    hj3SudoGwonParquetPath =  hj3ParquetPath + "sudoBeerMs.parquet"
    hj3SudoGwonXlsxPath = hj3XlsxPath + "sudoBeerMs.xlsx"

    // DK
    dkMasterPath = rootPath + dkPath + soolMasterPath
    dkResultPath = rootPath + dkPath + soolResultPath + ethDt + "/"
    dkWholeSalePath = dkMasterPath + "WHOLE_SALE.parquet"
    dkwMartBPath = dkResultPath + "mart_dk_beer.parquet"
    dkwMartWPath = dkResultPath + "mart_dk_whisky.parquet"
    comMasterDkPath = addressPath + "com_main_dk.parquet"
    dkMenuAnlysResDfPath = dkResultPath + "menuAnalysisResultDF.parquet"
    dkHstryBeerPath = dkResultPath + "history_Beer.parquet"
    dkHstryWhiskyPath = dkResultPath + "history_Whisky.parquet"
    dkViewResExcelBeerPath = dkResultPath + s"viewResult_EXCEL_${ethDt}_Beer.parquet"
    dkViewResExcelWhiskyPath = dkResultPath + s"viewResult_EXCEL_${ethDt}_Whisky.parquet"
    dkViewResExcelPath = dkResultPath + s"viewResult_EXCEL_${ethDt}.parquet"
    dkViewResWebBeerPath = dkResultPath + s"viewResult_WEB_${ethDt}_Beer.parquet"
    dkViewResWebWhiskyPath = dkResultPath + s"viewResult_WEB_${ethDt}_Whisky.parquet"
    dkViewResWebPath = dkResultPath + s"viewResult_WEB_${ethDt}.parquet"
    // HK
    hkMasterPath = rootPath + hkPath + soolMasterPath
    hkBranchDonePath = hkMasterPath + "branch_Done.parquet"
    hkResultPath = rootPath + hkPath + soolResultPath + ethDt + "/"
    hkrMartPath = hkResultPath + "martHK.parquet/*"
    hkwMartPath = hkResultPath + "martHK.parquet"
    hkBranchPath = hkResultPath + "branch_Done.parquet"
    hkDenoPath = hkResultPath + "denoDF.parquet"
    hkNumePath = hkResultPath + "numeDF.parquet"
    hkResPath = hkResultPath + "resultHK.parquet"
    hkMrtPath = hkResultPath + s"Heineken_${ethDt}.xlsx"

    // AngelNet
    angelItemPath = s3RootPath + "ANGELNET/ItemMaster/DKItemMaster_DictSize.parquet"
    angelItemHistPath = s3RootPath + "ANGELNET/" + ethDt + "/ItemMaster/DKItemMaster_DictSize.parquet"

    // master update
    itemMasterPath = rootPath + soolCommonPath + "ITEMMASTER/"
    afItemPath = itemMasterPath + "main.xlsx"
    ItemPath = rootPath + soolBasePath + ethDt + "/Model/final_main_df.parquet"
    // HJ master path
    hjBfPath = rootPath
    // DK master path
    bfDkbPath = dkMasterPath + "DKB_ITEM_MASTER.parquet"
    afDkbPath = itemMasterPath + "DKB_ITEM_MASTER.xlsx"
    bfDkwPath = dkMasterPath + "DKW_ITEM_MASTER.parquet"
    afDkwPath = itemMasterPath + "DKW_ITEM_MASTER.xlsx"
    dkMenuBfPath = dkMasterPath + "DK_MENU_ANALYSIS.parquet"
    dkMenuAfPath = itemMasterPath + "DK_MENU_ANALYSIS.xlsx"
    dkMenuhistPath = itemMasterPath + "BEFORE/" + ethDt + "/DK_MENU_ANALYSIS.parquet"
    dkbHistPath = itemMasterPath + "BEFORE/" + ethDt + "/DKB_ITEM_MASTER.parquet"
    dkwHistPath = itemMasterPath + "BEFORE/" + ethDt + "/DKW_ITEM_MASTER.parquet"
    // HJ
    bfHjsPath = hjMasterPath + "HJS_ITEM_MASTER.parquet"
    afHjsPath = itemMasterPath + "HJS_ITEM_MASTER.xlsx"
    bfHjbPath = hjMasterPath + "HJB_ITEM_MASTER.parquet"
    afHjbPath = itemMasterPath + "HJB_ITEM_MASTER.xlsx"
    hjCodeBfPath = hjMasterPath + "CD_DIMENSION.parquet"
    hjCodeAfPath = itemMasterPath + "CD_DIMENSION.xlsx"
    hj2ItemBfPath = hjMasterPath + "HJ2_ITEM.parquet"
    hj2ItemAfPath = itemMasterPath + "HJ2_ITEM.xlsx"
    hjsHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJS_ITEM_MASTER.parquet"
    hjbHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJB_ITEM_MASTER.parquet"
    hjCodeHistPath = itemMasterPath + "BEFORE/" + ethDt + "/CD_DIMENSION.parquet"
    hj2ItemHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJ2_ITEM.parquet"
    // HK
    bfHkCodePath = hkMasterPath + "main.xlsx"
    afHkCodePath = itemMasterPath + "main_hk.xlsx"
    hkHistPath = itemMasterPath + "BEFORE/" + ethDt + "/main_hk.parquet"
    // AngelNet
    angelItemPath = s3RootPath + "ANGELNET/ItemMaster/DKItemMaster_DictSize.parquet"
    angelItemHistPath = s3RootPath + "ANGELNET/" + ethDt + "/ItemMaster/DKItemMaster_DictSize.parquet"
    //결과 검증 데이터 결과 저장 경로
    dataCollectPath = verifyPath + "데이터수집량_공급업체이슈_" + ethDt + ".xlsx"
    dataCollectIssuePath = verifyPath + "data_clctn_issue_" + ethDt + ".parquet"
    dataCollectSupPath = verifyPath + "new_sup_issue_" + ethDt + ".parquet"
    missingSupPath = verifyPath + "missing_sup_issue_" + ethDt + ".parquet"
    verifyTimePath = verifyPath + s"시계열추이_${ethDt}.xlsx"
    verifyMartPath = verifyPath +  s"데이터마트_검증파일_${ethDt}.xlsx"
    maikDataPath = verifyPath + "mailData.xlsx"
    hkVerifyPath = hkResultPath + s"Heineken_verify_${ethDt}.xlsx"
  }
  else if (flag=="inv") {
    // s3 루트 경로
    s3RootPath = "s3a://sool/Invoice/"
    rootPath = s3RootPath
    // DTI 루트 경로
    rootDtiPath = "s3a://sool/"
    /****** DTI 경로 *******/
    // s3a://sool/Base/common/ 관련 파일 경로
    baseCmmnNonEthComPath = "s3a://sool/Base/common/" + "NonEthanolCom.parquet"
    nonCmmnEthItemPath = "s3a://sool/Base/common/" + "NonEthanolComItem.parquet"
    // s3a://sool/Base/집계연월/Model/ 관련 파일 경로
    modelPath = rootDtiPath + soolBasePath + ethDt + "/Model/"
    excelPath = rootDtiPath + soolBasePath + ethDt + "/Excel/"
    existedDfPath = modelPath + "existedDF.parquet"
    generatedDfPath = modelPath + "generatedDF.parquet"
    beforeDictDfPath = modelPath + "beforeDictDF.parquet"
    taggedDfPath = modelPath + "taggedDF.parquet"
    itemInfoBfPath = modelPath + "item_info_BEFORE.parquet"
    itemInfoAfPath = modelPath + "item_info_AFTER.parquet"
    itemInfoAfXlxPath = modelPath + "item_info_AFTER.xlsx"
    itemInfoFnlPath = modelPath + "item_info_FINAL.parquet"
    fnlNminfoDfPath = modelPath + "final_nminfo_df.parquet"
    // s3a://sool/Base/집계연월/Excel/ 관련 파일 경로
    itemInfoAfExcelPath = modelPath + "item_info_AFTER.xlsx"
    itemInfoFnlCsvPath = modelPath + "item_info_FINAL.csv"
    itemMatRstPath = modelPath + "item_matching_result.xlsx"
    // HJ 1
    hjFirstTradeListPath = "s3a://sool/HJ/Master/FIRST_TRADE_LIST1.parquet"
    hjSaUpJangPath = "s3a://sool/HJ/Master/SA_UP_JANG.parquet"
    // HJ 집계에 사용
    hjwLv2Path = "s3a://sool/HJ/Master/LV2W_MAP.parquet"
    hjSupCdDimensionPath = "s3a://sool/HJ/Master/SUP_CD_DIMENSION.parquet"
    hjCdAnvrParquetPath = "s3a://sool/HJ/Master/CD_ANVR.parquet"
    dtiCumulNminfoDfPath = "s3a://sool/Base/" + ethDt + "/Model/cumul_nminfo_df.parquet"
    dtiCumulNminfoDfNewPath = "s3a://sool/Base/" + ethDt + "/Model/NewCumul/cumul_nminfo_df.parquet"
//    fixAddrPath = "s3a://sool/COMMON/ADDRESS/fixAddr.parquet"

    /****** INV 경로 *****/
    // sool/COMMON/Address
    soolCommonAddressPath = rootPath + soolCommonPath + "ADDRESS/"
    cumulNminfoDfPath = "s3a://sool/Invoice/Base/" + ethDt + "/Model/cumul_nminfo_df.parquet"
    cumulNminfoDfNewPath = "s3a://sool/Invoice/Base/" + ethDt + "/Model/NewCumul/cumul_nminfo_df.parquet"
    invCumulNminfoDfPath = "s3a://sool/Invoice/Base/" + ethDt + "/Model/Tmp/cumul_nminfo_df.parquet"

    // 공통
    addressPath = rootPath + soolBasePath + ethDt + "/Address/"
    dataPath = rootPath + soolBasePath + ethDt + "/Data/"
    dataEnterprisePath = dataPath + "Enterprise/"
    verifyPath = rootPath + soolBasePath + ethDt + "/Verify/"

    // s3a://sool/Base/집계연월/Data/ 관련 파일 경로
    dtiPath = dataPath + "invoice.parquet"
    dtiEthPath = dataPath + "invEthanol.parquet"
    dtiCh3Path = dataPath + "invChannel3.parquet"
    dtiRtlPath = dataPath + "invRetail.parquet"
    nonEthComPath = dataPath + "NonEthanolCom.parquet"
    nonEthItemPath = dataPath + "NonEthanolComItem.parquet"
    basePath = dataPath + "inv_base.parquet"
    baseTaggedPath = dataPath + "inv_base_tagged.parquet"
    comMainPrePath = dataPath + "inv_com_main_pre.parquet"

    // s3a://sool/Base/집계연월/Data/Enterprise/ 관련 파일 경로
    vendorPath = dataEnterprisePath + "inv_vendor.parquet"
    retailPath = dataEnterprisePath + "inv_retail.parquet"
    newComPath = dataEnterprisePath + "inv_newCom.parquet"

    // s3a://sool/Base/집계연월/Address/ 관련 파일 경로
    rawAddrPath = addressPath + s"inv_RawAddr_${ethDt}.csv"
    bldngMgmtNmbAddrPath = addressPath + s"inv_bldngMgmtNmbAddr_${ethDt}.parquet"
    moAddrPath = addressPath + s"inv_MoAddr_${ethDt}.csv"
    addrMatchingPath = addressPath + s"inv_addrMatching_${ethDt}.parquet"
    bfComMasterPath = addressPath + "inv_bfComMaster.csv"
    comMainCsvPath = addressPath + "inv_com_main.csv"
    comMainPath = addressPath + "inv_com_main.parquet"
    comMainDkPath = addressPath + "inv_com_main_dk.parquet"
    comMasterDkPath = addressPath + "inv_com_main_dk.parquet"
    // HJ 1
    hjMasterPath = rootPath + hjPath + soolMasterPath
    hjResultPath = rootPath + hjPath + soolResultPath + ethDt + "/"
    hjCdDimensionPath = hjMasterPath + "CD_DIMENSION.parquet"
    hjrMartBPath = hjResultPath + "inv_mart_hite_beer.parquet/*"
    hjrMartSPath = hjResultPath + "inv_mart_hite_soju.parquet/*"
    hjwMartBPath = hjResultPath + "inv_mart_hite_beer.parquet" //inv save
    hjwMartSPath = hjResultPath + "inv_mart_hite_soju.parquet" //inv save

    hjwMartBExceptAddrPath = hjResultPath + "inv_mart_hite_non_addr_beer.parquet" //inv save
    hjwMartSExceptAddrPath = hjResultPath + "inv_mart_hite_non_addr_soju.parquet" //inv save

    hjAnvrSavePath = hjResultPath + "aggResult/parquet/"
    hjAnvr01Path = hjAnvrSavePath + "ANVR01.parquet"
    hjAnvr02Path = hjAnvrSavePath + "ANVR02.parquet"
    hjAnvr03Path = hjAnvrSavePath + "ANVR03.parquet"
    hjAnvr04Path = hjAnvrSavePath + "ANVR04.parquet"
    hjAnvr05Path = hjAnvrSavePath + "ANVR05.parquet"
    hjAnvr06Path = hjAnvrSavePath + "ANVR06.parquet"
    hjTsvSavePath = hjResultPath + "aggResult/hite" + ethDt + "/"

    hjSupMartBPath = hjResultPath + "supMart_hite_beer.parquet"
    hjSupMartSPath = hjResultPath + "supMart_hite_soju.parquet"
    hjSupDenoNumePath = hjResultPath + "aggResult/supParquet/hjSupDenoNume.parquet"
    hjAnvr13Path = hjTsvSavePath + "ANVR13.tsv"
    hjMartPath = hjResultPath + "mart_hite.parquet"
    // master update
    itemMasterPath = rootPath + soolCommonPath + "ITEMMASTER/"
    afItemPath = itemMasterPath + "main.xlsx"
    ItemPath = rootPath + soolBasePath + ethDt + "/Model/final_main_df.parquet"
    // HJ master path
    hjBfPath = rootPath
    // HJ
    bfHjsPath = hjMasterPath + "HJS_ITEM_MASTER.parquet"
    afHjsPath = itemMasterPath + "HJS_ITEM_MASTER.xlsx"
    bfHjbPath = hjMasterPath + "HJB_ITEM_MASTER.parquet"
    afHjbPath = itemMasterPath + "HJB_ITEM_MASTER.xlsx"
    hjCodeBfPath = hjMasterPath + "CD_DIMENSION.parquet"
    hjCodeAfPath = itemMasterPath + "CD_DIMENSION.xlsx"
    hj2ItemBfPath = hjMasterPath + "HJ2_ITEM.parquet"
    hj2ItemAfPath = itemMasterPath + "HJ2_ITEM.xlsx"
    hjsHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJS_ITEM_MASTER.parquet"
    hjbHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJB_ITEM_MASTER.parquet"
    hjCodeHistPath = itemMasterPath + "BEFORE/" + ethDt + "/CD_DIMENSION.parquet"
    hj2ItemHistPath = itemMasterPath + "BEFORE/" + ethDt + "/HJ2_ITEM.parquet"
    //결과 검증 데이터 결과 저장 경로
    dataCollectPath = verifyPath + "데이터수집량_공급업체이슈_" + ethDt + ".xlsx"
    dataCollectIssuePath = verifyPath + "data_clctn_issue_" + ethDt + ".parquet"
    dataCollectSupPath = verifyPath + "new_sup_issue_" + ethDt + ".parquet"
    missingSupPath = verifyPath + "missing_sup_issue_" + ethDt + ".parquet"
    verifyTimePath = verifyPath + s"시계열추이_${ethDt}.xlsx"
    verifyMartPath = verifyPath +  s"데이터마트_검증파일_${ethDt}.xlsx"
    maikDataPath = verifyPath + "mailData.xlsx"
    hkVerifyPath = hkResultPath + "Heineken_verify_" + ethDt + ".xlsx"

    //INV 2차
    hj2ComResPath = rootPath + "HJ2/Result/" + ethDt + "/Data/COMP_INFO.parquet"
    hj2AnlysResPath = rootPath + "HJ2/Result/" + ethDt + "/Data/ANALYSIS_RESULT.parquet"
    hj2ComResTsvPath = rootPath + "HJ2/Result/" + ethDt + "/File/COMP_INFO.tsv"
    hj2AnlysResTsvPath = rootPath + "HJ2/Result/" + ethDt + "/File/ANALIYSIS_RESULT.tsv"
    }
  else{
    println("error")
  }
}