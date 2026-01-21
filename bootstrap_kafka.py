from confluent_kafka.admin import AdminClient, NewTopic
from config import KAFKA_BROKER, KAFKA_TOPIC

def create_topic():
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    existing_topics = admin_client.list_topics(timeout=5).topics

    if KAFKA_TOPIC not in existing_topics:
        topic = NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"Topic '{KAFKA_TOPIC}' created")
    else:
        print(f"Topic '{KAFKA_TOPIC}' already exists")

if __name__ == "__main__":
    create_topic()
