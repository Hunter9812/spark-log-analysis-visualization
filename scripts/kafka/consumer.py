import argparse
from kafka import KafkaConsumer

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Kafka Consumer Script")
    parser.add_argument("--topic", default="default-topic", help="Kafka topic to subscribe to")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers (host:port)")
    parser.add_argument("--group-id", default="default-group", help="Kafka consumer group ID")

    args = parser.parse_args()

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        api_version=(4, 0, 0)
    )

    # Continuously listen for and print messages
    print(f"Listening to topic '{args.topic}' on {args.bootstrap_servers} as group '{args.group_id}'...")
    for message in consumer:
        print(message.value.decode("utf-8"))

if __name__ == "__main__":
    main()
