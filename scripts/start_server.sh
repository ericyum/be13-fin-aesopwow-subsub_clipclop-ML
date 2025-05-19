#!/bin/bash
set -euo pipefail
cd /home/ubuntu/app || exit 1

# 1. 기존 gunicorn 프로세스 종료
PID=$(pgrep -f 'gunicorn')
if [ -n "$PID" ]; then
  echo "기존 gunicorn 프로세스 종료: $PID"
  kill -9 $PID
  sleep 2
fi

# 2. 가상환경 활성화 (만약 사용한다면)
# source venv/bin/activate

# 3. 필요한 패키지 설치
pip3 install -r requirements.txt

# 4. Flask 앱 실행 (gunicorn)
nohup gunicorn --bind 0.0.0.0:8080 app:app > app.log 2>&1 &
