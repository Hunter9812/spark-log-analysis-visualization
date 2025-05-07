#!/bin/bash

# JAR 包路径
HDFS_JAR_PATH="hdfs://namenode:9000/spark/app.jar"
# Spark 集群主节点地址
SPARK_MASTER="spark://spark-master:7077"
# 部署模式
DEPLOY_MODE="cluster"

# 要运行的主类列表
MAIN_CLASSES=(
  "com.example.app.OdsBaseLogApp"
  "com.example.app.OdsBaseDbApp"
  "com.example.app.DwdDauApp"
  "com.example.app.DwdOrderApp"
)

# 循环提交每一个应用
for MAIN_CLASS in "${MAIN_CLASSES[@]}"; do
  echo "Submitting Spark application: $MAIN_CLASS"
  
  /spark/bin/spark-submit \
    --class "$MAIN_CLASS" \
    --master "$SPARK_MASTER" \
    --deploy-mode "$DEPLOY_MODE" \
    "$HDFS_JAR_PATH"

  if [ $? -ne 0 ]; then
    echo "Spark application $MAIN_CLASS failed. Stopping further submissions."
    exit 1
  else
    echo "$MAIN_CLASS submitted successfully."
  fi
done
