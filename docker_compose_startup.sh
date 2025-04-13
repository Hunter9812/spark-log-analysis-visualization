#!/bin/bash

# 手动加载 .env 文件
export $(grep -v '^#' ./docker/elastic/.env | xargs)

# 启动 compose
docker compose \
    -f ./docker/docker-hadoop-spark/docker-compose.yml \
    -f ./docker/elastic/docker-compose.yml \
    -f ./docker/kafka/docker-compose.yml \
    up -d
