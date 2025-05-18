#!/bin/bash

PID=$(pgrep -f 'java -jar')
if [ -n "$PID" ]; then
  echo "기존 Spring Boot 프로세스 종료: $PID"
  kill -9 $PID
  sleep 2
fi

echo "새로운 Spring Boot 앱 실행"
nohup java -jar /home/ubuntu/app/*.jar --spring.profiles.active=prod > /home/ubuntu/app/app.log 2>&1 &
