import argparse
import sys
import time
from pathlib import Path
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import (
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
    KafkaError,
)

BOOTSTRAP_SERVERS = "localhost:9092"


def with_admin_client(func):
    """decorator: Automatically create and close KafkaAdminClient"""
    def wrapper(*args, **kwargs):
        client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        try:
            return func(client, *args, **kwargs)
        finally:
            client.close()
    return wrapper


@with_admin_client
def create_topic(admin, topic, partitions=3, replication_factor=1):
    try:
        admin.create_topics([
            NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor)
        ])
        print(f"[✓] Created topic: {topic}")
    except TopicAlreadyExistsError:
        print(f"[!] Topic already exists: {topic}")


@with_admin_client
def delete_topic(admin, topic):
    try:
        admin.delete_topics([topic])
        print(f"[✓] Deleted topic: {topic}")
    except UnknownTopicOrPartitionError:
        print(f"[!] Topic does not exist: {topic}")


@with_admin_client
def list_topics(admin):
    topics = admin.list_topics()
    print("[Topic List]")
    for topic in topics:
        print(f" - {topic}")


def consume_topic(topic, latest=True):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset = 'latest' if latest else 'earliest'
    )
    print(f"[INFO] Consuming topic '{topic}' (latest={latest})...")
    try:
        for msg in consumer:
            print(msg.value.decode("utf-8", errors="ignore"))
    except KeyboardInterrupt:
        print("\n[INFO] Stopped consuming.")
    finally:
        consumer.close()


def produce_topic(topic, log_path):
    path = Path(log_path).resolve()
    if not path.exists():
        print(f"[ERROR] Log file does not exist: {path}", file=sys.stderr)
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8")
    )

    try:
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line:
                    producer.send(topic, value=line)
                    print(f"[SEND] {line}")
                    time.sleep(0.1)
        producer.flush()
    finally:
        producer.close()


@with_admin_client
def describe_topic(admin, topic):
    try:
        topic_info = admin.describe_topics([topic])
        for t in topic_info:
            print(f"Topic: {t['topic']}")
            print(f"  Internal: {t.get('is_internal', False)}")
            partitions = t.get('partitions', [])
            print(f"  Partitions: {len(partitions)}")
            for p in sorted(partitions, key=lambda x: x["partition"]):
                print(f"    Partition {p['partition']} -> Leader: {p['leader']}, "
                      f"Replicas: {p['replicas']}, ISR: {p['isr']}", end="")
                if p.get("offline_replicas"):
                    print(f", Offline: {p['offline_replicas']}")
                else:
                    print()
    except UnknownTopicOrPartitionError:
        print(f"[!] Topic does not exist: {topic}")
    except KafkaError as e:
        print(f"[ERROR] Kafka error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Kafka Topic Management Tool")
    subparsers = parser.add_subparsers(dest="command")

    # Create
    create_parser = subparsers.add_parser("create", help="Create a Kafka topic")
    create_parser.add_argument("topic")
    create_parser.add_argument("--partitions", type=int, default=1)
    create_parser.add_argument("--replication", type=int, default=1)

    # Delete
    delete_parser = subparsers.add_parser("delete", help="Delete a Kafka topic")
    delete_parser.add_argument("topic")

    # List
    subparsers.add_parser("list", help="List all Kafka topics")

    # Consume
    consume_parser = subparsers.add_parser("consume", help="Consume messages from a topic")
    consume_parser.add_argument("topic")
    consume_parser.add_argument(
        "--earliest",
        action="store_true",
        help="Consume messages from the earliest offset (auto_offset_reset='earliest'); default is latest"
    )

    # Produce
    produce_parser = subparsers.add_parser("produce", help="Produce messages from a log file")
    produce_parser.add_argument("topic")
    produce_parser.add_argument("--log-path", required=True)

    # Describe
    describe_parser = subparsers.add_parser("describe", help="Describe a Kafka topic")
    describe_parser.add_argument("topic")

    args = parser.parse_args()

    if args.command == "create":
        create_topic(args.topic, args.partitions, args.replication)
    elif args.command == "delete":
        delete_topic(args.topic)
    elif args.command == "list":
        list_topics()
    elif args.command == "consume":
        consume_topic(args.topic, not args.earliest)
    elif args.command == "produce":
        produce_topic(args.topic, args.log_path)
    elif args.command == "describe":
        describe_topic(args.topic)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
