
import os

ENV = os.getenv("ENVIRONMENT", "dev")

if ENV == "dev":
    KAFKA_BROKER = "kafka:9092"
    SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
else:
    KAFKA_BROKER = "localhost:9092"
    SCHEMA_REGISTRY_URL = "http://localhost:8081"

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices-avro")
