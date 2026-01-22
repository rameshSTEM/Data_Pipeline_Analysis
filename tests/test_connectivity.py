import requests
from confluent_kafka import Producer, KafkaException

# Configuration
KAFKA_BROKER = "kafka:9092" # container network name
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Test Schema Registry
try:
    response = requests.get(SCHEMA_REGISTRY_URL, timeout=5)
    if response.status_code == 200:
        print(f"Schema Registry reachable at {SCHEMA_REGISTRY_URL}")
    else:
        print(f"Schema Registry returned status {response.status_code}")
except requests.RequestException as e:
    print(f"Could not reach Schema Registry: {e}")

# Test Kafka Broker
try:
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    print(f"Kafka broker reachable at {KAFKA_BROKER}")
except KafkaException as e:
    print(f"Could not connect to Kafka: {e}")
