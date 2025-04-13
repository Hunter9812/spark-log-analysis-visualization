# coding: utf-8
import sys
import time
import argparse
from pathlib import Path
from kafka import KafkaProducer

# 解析命令行参数
parser = argparse.ArgumentParser(description='Send logs to Kafka')
parser.add_argument('--log-path', required=True, help='Log file path')
parser.add_argument('--topic', default='default-topic', help='Kafka topic')
args = parser.parse_args()

# 获取日志文件路径
log_path = Path(args.log_path).resolve()
topic = args.topic

# Kafka配置
bootstrap_servers = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    api_version=(4,0,0),
    value_serializer=lambda v: v.encode("utf-8")  # 把字符串转为字节
)

# 检查文件是否存在
if not log_path.exists():
    print(f"[错误] 日志文件不存在: {log_path}", file=sys.stderr)
    sys.exit(1)

# 打开并逐行发送日志
with open(log_path, "r", encoding="utf-8") as logfile:
    for line in logfile:
        line = line.strip()
        if not line:
            continue  # 忽略空行
        producer.send(topic, value=line)
        print(f"[发送] {line}")
        time.sleep(0.1)  # 模拟“实时”发送，控制频率

producer.flush()
producer.close()
