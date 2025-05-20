#!/bin/bash
set -euo pipefail

cd /home/ubuntu/app || exit 1

# 권한 부여 및 로그 파일 생성
sudo chmod a+w /home/ubuntu/app
touch gunicorn_access.log gunicorn_error.log

# 1. 기존 gunicorn 프로세스 종료
PID=$(pgrep -f 'gunicorn') || true
if [ -n "${PID:-}" ]; then
  echo "기존 gunicorn 프로세스 종료: $PID"
  kill -9 $PID
  sleep 2
fi

# 2. 가상환경 활성화
#source /home/ubuntu/venv/bin/activate

# 3. 패키지 설치 (가상환경에)
pip install -r requirements.txt

# 4. Flask 앱 실행 (gunicorn)
# 🔥 파일명이 app.py라면 아래처럼 app:app
nohup gunicorn --config /home/ubuntu/app/gunicorn_config.py app:app > /home/ubuntu/app/app.log 2>&1 &

