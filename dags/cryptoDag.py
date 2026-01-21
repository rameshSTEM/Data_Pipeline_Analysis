import json
import time
import requests
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'crypto_data'

#CoinGecko API endpoint for cryptocurrency data
COINGECKO_API_URL = 'https://api.coingecko.com/api/v3/coins/markets'
PARAMS = {
    'vs_currency': 'usd',
    'ids': 'bitcoin,ethereum,solana,cardano,ripple,dogecoin,polkadot,binancecoin,avalanche,chainlink,polygon,cosmos,uniswap,litecoin,stellar,vechain,shiba-inu,tron,tezos,neo',
    'order': 'market_cap_desc',
    'per_page': 20,
    'page': 1,
    'sparkline': 'false', #avoid fetching sparkline data
    'price_change_percentage': '24h' #api query config
}

# Desired keys to extract
desired_keys = [
    "id", "symbol", "current_price", "market_cap",
    "total_volume", "high_24h", "low_24h", "last_updated"
]

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))





