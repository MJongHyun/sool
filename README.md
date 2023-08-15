# 주류 서비스

### 요약
    + service
        - 주류 고객사별 마트 데이터 생성 및 저장
        - 주류 고객사별 집계 데이터 생성 및 저장

    + address
        - 주류 주소 정제 결과 생성
    
    + communication (2022.06.08 기준, 사용 안하는 중)
        - 주류 고객사별 커뮤니케이션 엑셀 생성
    
    + extract (2022.06.08 기준, 사용 안하는 중)
        - 주류 집계에 필요한 각종 데이터 생성

    (2022.09 생성 중)    
    + master 
        - 주류 집계에 필요한 Master file 업데이트

    + extract 
        - 비주류 업체, 비주류 아이템 데이터 생성
        - 주류 데이터 추출
        - 검증 파일 생성
    + service/item_tagging
        - 아이템 태깅 관련 파일 생성
        - 수동 아이테 태깅 후 데이터 업데이트 및 검증 파일 저  


### 현재상황
    - 신규 주류 서버(ubuntu@0.0.0.0)에서 매달 주류 운영 시 사용 중인 코드
    - 마트 데이터 생성, 집계 데이터 생성, 주소 정제 결과 생성을 위해 사용 중장
    - (2022.09) 제플린에서 수동실행도 jar파일로 실행할 수 있도록 추가중

### 코드 및 스크립트 설명
    + 패키지:
        sool.address: 주소 정제 관련 패키지
        sool.common: 공통 사용 코드들 모음
        sool.communication: 고객사별 커뮤니케이션 엑셀 생성
        sool.service: 고객사 마트 데이터, 집계 데이터 생성 및 저장
        sool.extract: 도매상 리스트 업데이트, 소매 데이터 추출, 유흥 데이터 추출, 소매상 리스트 업데이트, 분석 기초 데이터 생성
        sool.master : 주류 집계에 필요한 Master file 업데이트

    + 스크립트:
        makeJar.sh: sbt assembly 및 주류 서버, 58번 서버로 jar 파일 전송하는 스크립트

### 실행 방법
    1. spark-submit
         
        + service
            - 인자값 리스트
                args(0): 집계연월 (yyyymm)
                args(1): 결과타입. 마트를 생성할 것인지, 집계 결과를 생성할 것인지 (mart, agg)
                args(2): 고객사명 (hj, hj2, dk, hk, anglnt, all)

            - 인자값 예시
                20220608 mart all(전체 실행)
                20220608 agg all(전체 실행)
                20220608 mart hj
                20220608 agg dk
                :
            
                * 20220608 기준, 현재 마트 생성(mart), 집계 결과 생성(agg) 둘 다 all 인자값 사용중

            - 기본 형식
                spark-submit --class sool.service.run.RunService {jar 파일 경로} {집계연월} {결과타입} {고객사명}

            - 신규 주류 서버
                spark-submit \
                --master local[6] \
                --driver-memory 12g \
                --conf spark.driver.maxResultSize=4g \
                --conf spark.debug.maxToStringFields=100 \
                --driver-java-options "-Dlog4j.configuration=log4j.properties" \
                --driver-class-path /home/ubuntu/jar/sool/sool.jar \
                --class sool.service.run.RunService \
                /home/ubuntu/jar/sool/sool.jar 202108 agg hj
            
            - 신규 주류 서버 (local log4j.properties 사용 시)
                spark-submit \
                --master local[6] \
                --driver-memory 12g \
                --conf spark.driver.maxResultSize=4g \
                --conf spark.debug.maxToStringFields=100 \
                --driver-java-options "-Dlog4j.configuration=file:/home/ubuntu/jar/sool/log4j.properties" \
                --class sool.service.run.RunService \
                /home/ubuntu/jar/sool/sool.jar 202108 mart dk

        + communication
            - 기본 형식
                spark-submit --class sool.communication.run.RunCommunication {jar 파일 경로} {고객사명}

        + address
            - Object 리스트
                sool.address.run.RunBldngMgmtNmbAddr: 다음 API를 통해 각 원천 주소에 해당하는 건물관리번호를 구하여 DB 에 저장하는 코드
                sool.address.run.RunComMain: 집단 데이터 추출, 사업자별 대표 주소 선정 및 결과 저장, 업체 마스터 생성 및 저장
                sool.address.run.RunBldngMgmtNmbAddr2: 다음 API 대용으로 API 웹 사이트 주소 막힐 시 우회하여 동작하는 코드

            - 신규 주류 서버 (주소 실행)
                spark-submit \
                --master local[6] \
                --driver-memory 12g \
                --conf spark.driver.maxResultSize=4g \
                --conf spark.debug.maxToStringFields=100 \
                --driver-java-options "-Dlog4j.configuration=log4j.properties" \
                --driver-class-path /home/ubuntu/jar/sool/sool.jar \
                --class sool.address.run.RunComMain \
                /home/ubuntu/jar/sool/sool.jar {집계연월}

        + extract
            - 인자값 리스트
                args(0): 집계연월 (yyyymm)

            - 기본 형식
                spark-submit --class sool.extract.run.RunExtract {jar 파일 경로} {집계연월} 

            - 신규 주류 서버
                spark-submit \
                --master local[6] \
                --driver-memory 12g \
                --conf spark.driver.maxResultSize=4g \
                --conf spark.debug.maxToStringFields=100 \
                --driver-java-options "-Dlog4j.configuration=log4j.properties" \
                --driver-class-path /home/ubuntu/jar/sool/sool.jar \
                --class sool.extract.run.RunExtract \
                /home/ubuntu/jar/sool/sool.jar 202110
      
    2. spark-shell

        + service, communication, address, extract
            - 기본 형식
                spark-shell --jars {jar 파일 경로}

            - 신규 주류 서버
                spark-shell \
                --master local[6] \
                --driver-memory 8g \
                --conf spark.driver.maxResultSize=4g \
                --conf spark.debug.maxToStringFields=100 \
                --driver-java-options "-Dlog4j.configuration=log4j.properties" \
                --driver-class-path /home/ubuntu/jar/sool/sool.jar \
                --jars /home/ubuntu/jar/sool/sool.jar
  
### 버전관리
    ver 0.0.1: 코드 생성 완료
    ver 0.1.0: 거래데이터 데이터에서 필요 데이터 추출하는 코드 생성
    ver 0.2.0: 코드 사용 권한 IP 설정
    ver 0.3.0: sbt, sbt-assembly 버전 업그레이드, library dependency 수정
    ver 0.3.1: AG 코드 일부 수정
    ver 0.4.0: 데이터 추출 부분 코드 생성중(dtiEthanol, rawAddr 추출) 
    ver 0.4.1: HJ 2차 업체 추출 기준 변경으로 인한 코드 수정
    ver 0.4.2: 데이터 추출 부분 코드 생성중(vendorRaw, dtiRetail, dtiCh3 추출)
    ver 0.4.3: 일부 코드 수정
    ver 0.4.4: 주석 일부 수정
    ver 0.4.5: 데이터 추출 부분 코드 생성중(retail, vendor, base 추출)
    ver 0.4.6: 서비스 실행 시 타임스탬프 출력하는 기능 추가
    ver 0.4.7: DK TEST DB 접근 코드 추가
    ver 0.4.8: 데이터 추출 부분 코드 생성중(moAddr 추출)
    ver 0.4.9: 파일 관련 코드 수정
    ver 0.4.10: HJ 결과 생성 이슈 대응(기존 술 서버 spark 버전과 코드 실행한 spark 버전이 달라서 null 값이 "" 혹은 빈칸으로 생성되던 이슈)
    ver 0.4.11: 데이터 추출 부분 코드 생성중(moAddr 추출), s3 데이터 존재 여부 확인 코드 추가, spark conf 수정
    ver 0.4.12: 패키지명 코드 컨벤션에 맞게 수정
    ver 0.4.13: 변수명 일부 수정
    ver 0.4.14: 데이터 추출 부분 코드 생성중(머신러닝 훈련용 파일 생성 부분)
    ver 0.4.15: 메인 함수 조건에 따라 조기 리턴되도록 변경
    ver 0.4.16: 데이터 추출 부분 코드 생성중(최초 품목 사전(item_info_BEFORE.parquet) 생성)
    ver 0.4.17: 데이터 추출 부분 코드 생성중(최종 품목 사전(item_info_AFTER.parquet) 생성)
    ver 0.4.18: 데이터 추출 부분 코드 생성중(아이템 태깅 완료된 아이템 품목 사전(item_info_FINAL.parquet, final_nminfo_df.parquet) 생성)
    ver 0.4.19: 데이터 추출 부분 코드 생성중(누적 품목사전 업데이트)
    ver 0.4.20: 주류 운영 신규 서버 IP 추가
    ver 0.4.21: DK 집계 관련 코드 수정(기네스마이크로드래프트558 관련)
    ver 0.4.22: DK 집계 관련 코드 수정(기네스 & 홉하우스13 관련 예외 메뉴 리스트 제외하던 코드 관련)
    ver 0.4.23: HK 집계 관련 코드 수정(매입 업체에서 고래맥주창고 관련 사업자 제외)
    ver 0.4.24: HK 집계 관련 코드 수정(고래맥주창고 DB 테이블 생성으로 인한 코드 반영)
    ver 0.4.25: 로그 관련 코드 수정
    ver 0.4.26: 자체 내장 log4j.properties 사용하도록 log4j.properties 수정 및 관련 코드 수정
    ver 0.4.27: jar 파일 내 log4j.properties 사용 목적 기타 수정
    ver 0.4.28: HJ 2차 분모가 0 일 때 MS 를 0 으로 처리(21년 6월 이전 코드)
    ver 0.4.29: DB 변경 코드 적용(기존 주류 서버 ---> 신규 주류 서버)
    ver 0.4.30: 로그 파일에 에러 로그도 포함시키기 및 메인 로그 일부 수정
    ver 0.5.0: 주소 정제 코드 생성
    ver 0.5.1: 주소 정제 결과 DB Upsert 기능 추가
    ver 0.5.2: 주소 정제 코드 로그 추가
    ver 0.5.3: HJ 2차 기준 변경으로 인한 코드 수정
    ver 0.5.4: 주소 정제 결과 DB Upsert 시 에러 대응
    ver 0.5.5: 주소 매칭 코드 추가 및 주소 정제 코드 수정
    ver 0.5.6: 건물관리번호 수집 및 DB Upsert 코드 수정
    ver 0.5.7: 건물관리번호 수집 코드, 건물관리번호 매칭 코드 등 수정
    ver 0.5.8: 각 사업자별 대표 건물관리번호 추출
    ver 0.5.9: 건물관리번호 수집 및 DB Upsert 코드 수정
    ver 0.5.10: 사업자별 대표 건물관리번호, 사업자명 추출 코드 생성
    ver 0.5.11: 건물정보 DB read jdbc partiton 추가
    ver 0.5.12: 사업자별 대표 주소 추출 코드 생성
    ver 0.5.13: 사업자별 건물관리번호 개수 컬럼 "LEN" 추가
    ver 0.5.14: 사업자별 대표 주소 추출 코드 수정
    ver 0.5.15: 원천 주소별 건물관리번호 수집 코드 수정
    ver 0.5.16: 사업자별 대표 주소 추출 코드 수정(지번, 건물 본부번에 -0 부분 제거)
    ver 0.5.17: 업체 마스터 생성 코드 추가
    ver 0.5.18: 사업자별 대표 주소 추출 코드 수정(시군구명 null 값을 "" 값으로 대체)
    ver 0.5.19: 업체 마스터 생성 코드 수정
    ver 0.5.20: 원천 주소 추출 코드를 원천 주소별 건물관리번호 수집 코드로 통합
    ver 0.5.21: 모집단 추출 코드를 업체 마스터 생성 코드로 통합
    ver 0.5.22: 업체 마스터 생성 코드 수정
    ver 0.5.23: DK WEB, EXCEL 결과 DK TEST 서버 DB 에 저장하는 코드 생성
    ver 0.6.0: 도매상 리스트 업데이트, 소매 데이터 추출, 유흥 데이터 추출, 소매상 리스트 업데이트, 분석 기초 데이터 생성
    ver 0.6.1: 마트, 집계 데이터 생성 시, 한 번에 전체 고객사 데이터 생성하는 기능 추가
    ver 0.6.2: DK 집계 코드 직렬화 관련 에러 발생 대응
    ver 0.6.3: DK 집계 코드 내 함수명 수정
    ver 0.6.4: JDBC 관련 코드 수정(conn, stmt close)
    ver 0.7.0: zeppelin 코드 이전
    ver 0.7.1: zeppelin 3-7까지 코드 작성
    ver 0.7.2: zeppelin 코드 1차 반영
    ver 0.7.3: dk db 변경 및 dtiEthanol 추출 코드 수정
    ver 0.7.4: 주소 스크래핑 코드 추가 (다음 카카오 오류시 실행가능하도록)
    ver 0.7.5: inv마트 생성 정규표현식 수정, 고정주소 DB 변경, HJ3차코드 패킹, 주류 데이터 추출 코드 변경
    ver 0.7.6: db에서 data load시 trim, HJ 메뉴 쿼리 검증 프로세스 추가 
    ver 0.7.7: slack 알림 추가
    