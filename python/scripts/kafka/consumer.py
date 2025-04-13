from kafka import KafkaConsumer

bootstrap_servers = 'localhost:9092'

consumer = KafkaConsumer(
    'default-topic',
    bootstrap_servers=bootstrap_servers,
    api_version=(4,0,0),
    group_id='my-group'  # 使用消费者组以便于协同消费消息
)

# 持续监听并打印接收到的消息
for message in consumer:
    print(message.value.decode("utf-8"))
