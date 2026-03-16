import os
import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
NEWS_TOPIC = os.getenv("NEWS_TOPIC")

TICKERS = {
    "AAPL": "Apple stock",
    "GOOGL": "Google Alphabet stock",
    "MSFT": "Microsoft stock",
    "AMZN": "Amazon stock",
    "TSLA": "Tesla stock",
    "META": "Meta Facebook stock",
    "NVDA": "NVIDIA stock"
}

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def fetch_news(ticker):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": TICKERS[ticker],
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 5,
        "apiKey": NEWS_API_KEY
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("articles", [])

def produce_news():
    producer = create_producer()
    print(f"[{datetime.now()}] Starting news producer...")

    while True:
        for ticker in TICKERS.keys():
            try:
                articles = fetch_news(ticker)
                for article in articles:
                    message = {
                        "ticker": ticker,
                        "title": article.get("title"),
                        "description": article.get("description"),
                        "url": article.get("url"),
                        "published_at": article.get("publishedAt"),
                        "source": article.get("source", {}).get("name"),
                        "ingested_at": datetime.now().isoformat()
                    }
                    producer.send(NEWS_TOPIC, value=message)
                    print(f"[{datetime.now()}] Sent: {ticker} - {article.get('title', '')[:50]}")

                producer.flush()

            except Exception as e:
                print(f"[{datetime.now()}] Error for {ticker}: {e}")

        print(f"[{datetime.now()}] Cycle complete. Sleeping 15 minutes...")
        time.sleep(900)

if __name__ == "__main__":
    produce_news()