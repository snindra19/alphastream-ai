import os
import json
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
NEWS_TOPIC = os.getenv("NEWS_TOPIC")
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
NEWS_TABLE = os.getenv("NEWS_TABLE")

TABLE_ID = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{NEWS_TABLE}"

def create_consumer():
    return KafkaConsumer(
        NEWS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="news-consumer-group"
    )

def create_bigquery_client():
    return bigquery.Client(project=BIGQUERY_PROJECT)

def ensure_table_exists(client):
    schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("published_at", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("ingested_at", "STRING"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"Table {TABLE_ID} ready.")

def consume_news():
    consumer = create_consumer()
    bq_client = create_bigquery_client()
    ensure_table_exists(bq_client)

    print(f"[{datetime.now()}] Starting news consumer...")

    rows_batch = []

    for message in consumer:
        try:
            row = message.value
            rows_batch.append(row)
            print(f"[{datetime.now()}] Consumed: {row.get('ticker')} - {row.get('title', '')[:50]}")

            if len(rows_batch) >= 10:
                errors = bq_client.insert_rows_json(TABLE_ID, rows_batch)
                if errors:
                    print(f"BigQuery insert errors: {errors}")
                else:
                    print(f"[{datetime.now()}] Inserted {len(rows_batch)} rows to BigQuery")
                rows_batch = []

        except Exception as e:
            print(f"[{datetime.now()}] Error: {e}")

if __name__ == "__main__":
    consume_news()