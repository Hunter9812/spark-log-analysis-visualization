#!/bin/bash

# 检查参数数量
if [ $# -lt 1 ]; then
  echo "用法: $0 [app|db] [mock.date]"
  exit 1
fi

SERVICE=$1
MOCK_DATE=$2

case "$SERVICE" in
  app)
    cd applog || { echo "切换到目录 applog 失败"; exit 1; }
    if [ -n "$MOCK_DATE" ]; then
      sed -i "/mock.date/c mock.date: $MOCK_DATE" application.yml
    fi
    java -jar gmall2020-mock-log-2021-11-29.jar
    ;;
  db)
    cd db_log || { echo "切换到目录 db_log 失败"; exit 1; }
    if [ -n "$MOCK_DATE" ]; then
      sed -i "/mock.date/c mock.date: $MOCK_DATE" application.properties
    fi
    java -jar gmall2020-mock-db-2021-01-22.jar
    ;;
  *)
    echo "无效的服务类型: $SERVICE，应为 app 或 db"
    exit 1
    ;;
esac
