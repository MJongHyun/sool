#! /bin/sh

sbt assembly

#신규 주류 서버
scp -i /Users//pem/service-sool.pem /Users//IdeaProjects/sool/target/scala-2.11/sool.jar ubuntu@0.0.0.0:/home/ubuntu/jar/sool/
echo "new sool 서버에 sool.jar 를 전송했습니다."

#58번 서버
#진행 상황이 안나오는 현상 발생
sshpass -p '@8893' scp -o StrictHostKeyChecking=no /Users//IdeaProjects/sool/target/scala-2.11/sool.jar @211.180.114.58:/home//jar/sool/
#scp -o StrictHostKeyChecking=no /Users//IdeaProjects/sool/target/scala-2.11/sool.jar @211.180.114.58:/home//jar/sool/
echo "58번 서버에 sool.jar 를 전송했습니다."

# 재택근무 때 사용
#scp -i /Users//pem/emergency.pem /Users//IdeaProjects/sool/target/scala-2.11/sool.jar ubuntu@3.36.234.67:/home/ubuntu//
#echo "aws 서버에 sool.jar 를 전송했습니다."

#기존 주류 서버
#scp -i /Users//pem/Keta-hadoop.pem /Users//IdeaProjects/sool/target/scala-2.11/sool.jar ubuntu@52.79.224.250:/home/ubuntu/jar/sool/
#echo "sool 서버에 sool.jar 를 전송했습니다."