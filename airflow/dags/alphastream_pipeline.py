from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

default_args = {
    'owner': 'alphastream',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def run_news_producer():
    import requests
    import json
    import os
    from kafka import KafkaProducer
    from dotenv import load_dotenv

    load_dotenv('/opt/airflow/dags/../.env')

    NEWS_API_KEY = os.getenv("NEWS_API_KEY")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    NEWS_TOPIC = os.getenv("NEWS_TOPIC", "news.events")

    TICKERS = {
        "AAPL": "Apple stock",
        "GOOGL": "Google Alphabet stock",
        "MSFT": "Microsoft stock",
        "AMZN": "Amazon stock",
        "TSLA": "Tesla stock",
        "META": "Meta Facebook stock",
        "NVDA": "NVIDIA stock"
    }

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for ticker, query in TICKERS.items():
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": query,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 5,
                "apiKey": NEWS_API_KEY
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            articles = response.json().get("articles", [])

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

            producer.flush()
            print(f"Sent {len(articles)} articles for {ticker}")

        except Exception as e:
            print(f"Error for {ticker}: {e}")
            raise

with DAG(
    dag_id='alphastream_pipeline',
    default_args=default_args,
    description='AlphaStream news ingestion and transformation pipeline',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 3, 16),
    catchup=False,
    tags=['alphastream', 'news', 'kafka', 'dbt'],
) as dag:

    ingest_news = PythonOperator(
        task_id='ingest_news',
        python_callable=run_news_producer,
    )

    run_dbt = BashOperator(
        task_id='run_dbt_transforms',
        bash_command='cd /opt/airflow/dbt_project/alphastream && dbt run --no-partial-parse --profiles-dir /opt/airflow/dbt_project',
    )

    test_dbt = BashOperator(
        task_id='test_dbt_models',
        bash_command='cd /opt/airflow/dbt_project/alphastream && dbt test --no-partial-parse --profiles-dir /opt/airflow/dbt_project',
    )

    ingest_news >> run_dbt >> test_dbt