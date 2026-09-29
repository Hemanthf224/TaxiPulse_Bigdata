import os
import time
import json
import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "taxi-trip-events"
DATA_FILE = "C:/TaxiPulse_Bigdata/data/raw/yellow/year=2023/month=08/yellow_tripdata_2023-08.parquet"

def get_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_data():
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    try:
        producer = get_producer()
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    print(f"Reading {DATA_FILE}...")
    try:
        # Read a chunk of data (e.g., first 10,000 rows)
        df = pd.read_parquet(DATA_FILE).head(10000)
    except Exception as e:
        print(f"Failed to read parquet file: {e}")
        return

    # Convert datetime to string for JSON serialization
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].astype(str)
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].astype(str)

    print("Streaming data to Kafka...")
    count = 0
    for _, row in df.iterrows():
        record = row.to_dict()
        producer.send(TOPIC, value=record)
        count += 1
        
        # Print progress and simulate a fast stream (10x real-time or just 0.01s delay)
        if count % 100 == 0:
            print(f"Sent {count} events...")
        time.sleep(0.01)

    producer.flush()
    print(f"Finished streaming {count} events.")

if __name__ == "__main__":
    stream_data()
