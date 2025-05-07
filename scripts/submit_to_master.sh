#!/bin/bash

# 容器名
NAMENODE_CONTAINER="namenode"
SPARK_CONTAINER="spark-master"
VALKEY_CONTAINER="valkey"
MAXWELL_CONTAINER="maxwell"

LOCAL_SCRIPT="run_all_apps.sh"
LOCAL_JAR="../spark-realtime/sparkStreaming-realtime/target/sparkStreaming-realtime-1.0-SNAPSHOT-jar-with-dependencies.jar"

REMOTE_SCRIPT="/root/run_all_apps.sh"
REMOTE_JAR="/root/app.jar"
REMOTE_HDFS_DIR="/spark/"

cd "$(dirname "$0")"

echo -e "\nStep: Adding table names to Valkey..."
docker exec "$VALKEY_CONTAINER" valkey-cli sadd FACT:TABLES order_info order_detail
docker exec "$VALKEY_CONTAINER" valkey-cli sadd DIM:TABLES user_info base_province

echo -e "\nStep: Uploading Elasticsearch templates..."
./upload-es-templates.sh es-templates

echo -e "\nStep: Deploy and run Spark script inside container..."
docker cp "$LOCAL_SCRIPT" "$SPARK_CONTAINER":"$REMOTE_SCRIPT"
docker cp "$LOCAL_JAR" "$NAMENODE_CONTAINER":"$REMOTE_JAR"
docker exec "$NAMENODE_CONTAINER" bash -c "
hdfs dfs -mkdir -p $REMOTE_HDFS_DIR &&
hdfs dfs -put $REMOTE_JAR $REMOTE_HDFS_DIR &&
hdfs dfs -ls $REMOTE_HDFS_DIR
"
docker exec -it "$SPARK_CONTAINER" chmod +x "$REMOTE_SCRIPT"
docker exec -it "$SPARK_CONTAINER" "$REMOTE_SCRIPT"

echo -e "Bootstrapping historical dimension data via Maxwell..."
docker exec "$MAXWELL_CONTAINER" bin/maxwell-bootstrap --database gmall --table base_province --config config.properties
# Note: This bootstrap command is redundant during the initial SQL setup, as the user_info table is empty.
# However, it becomes useful if the system has been started before and the table contains data.
docker exec "$MAXWELL_CONTAINER" bin/maxwell-bootstrap --database gmall --table user_info --config config.properties

