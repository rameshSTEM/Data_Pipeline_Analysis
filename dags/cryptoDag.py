import time
import requests
from confluent_kafka.avro import AvroProducer, loads
#Exception class for Kafka-specific runtime errors.
from confluent_kafka import KafkaException


#kafka and schema registry settings
KAFKA_BROKER = 'kafka:9092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
KAFKA_TOPIC = 'crypto-prices-avro'
POLL_INTERVAL = 30  # seconds

#REST enpoint to fetch crypto data
COINGECKO_URL = 'https://api.coingecko.com/api/v3/coins/markets'

#parameters for the API request
PARAMS = {
    'vs_currency': 'usd',
    'ids': 'bitcoin,ethereum,solana,cardano,ripple,dogecoin,polkadot,binancecoin,avalanche,chainlink,polygon,cosmos,uniswap,litecoin,stellar,vechain,shiba-inu,tron,tezos,neo',
    'order': 'market_cap_desc',
    'per_page': 20,
    'page': 1,
    'sparkline': 'false',
    'price_change_percentage': '24h'
}
#keys to extract from the API response
DESIRED_KEYS = [
    "id", "symbol", "current_price", "market_cap",
    "total_volume", "high_24h", "low_24h", "last_updated"
]

# Avro Schema definition and loading
VALUE_SCHEMA = loads("""
{
  "namespace": "crypto.avro",
  "type": "record",
  "name": "CryptoCoin",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "symbol", "type": "string"},
    {"name": "current_price", "type": "double"},
    {"name": "market_cap", "type": "double"},
    {"name": "total_volume", "type": "double"},
    {"name": "high_24h", "type": "double"},
    {"name": "low_24h", "type": "double"},
    {"name": "last_updated", "type": "string"}
  ]
}
""")


# Kafka Producer Initialization
producer = AvroProducer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "acks": "all",
        "retries": 5,
        "linger.ms": 100,
    },
    default_value_schema=VALUE_SCHEMA #set the default value schema for all the messages produced
)


# Callback function executed after message delivery attempt
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to {msg.topic()} " #logs topic assigned
            f"[partition {msg.partition()}]" # logs partition assigned
            f" at offset {msg.offset()}"    # logs offset assigned
        )


# Data Fetching
def fetch_crypto_data():
    response = requests.get(
        COINGECKO_URL,
        params=PARAMS,
        timeout=10
    )
    response.raise_for_status()

    return [
        {key: coin.get(key) for key in DESIRED_KEYS}
        for coin in response.json()
    ]


# Streaming Loop
def stream_crypto_data():
    print("Streaming crypto prices to Kafka (Avro)...")

    while True:
        try:
            records = fetch_crypto_data()

            for record in records:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    value=record,
                    on_delivery=delivery_report
                )

            producer.poll(0)  # Trigger callbacks
            print(f"Sent {len(records)} records") # log batch size

        except (KafkaException, requests.RequestException) as e:
            print(f"Error: {e}")

        time.sleep(POLL_INTERVAL)

# Entrypoint
if __name__ == "__main__":
    try:
        stream_crypto_data()
    except KeyboardInterrupt:
        print("\n Shutting down...")
    finally:
        # Ensure all buffered messages are delivered.
        producer.flush()
        print("Producer closed cleanly")
