import json
import time
import pandas as pd
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "yellow_taxi"
PARQUET_FILE = "/opt/airflow/data/raw/yellow/2019/yellow_tripdata_2019-02.parquet"

STREAM_MODE = "LOW"  # Change this per scenario: LOW, NORMAL, HIGH, BURST, RANDOM

print("Connecting to Kafka...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Reading parquet file...")

SIZE = 1000

df = pd.read_parquet(
    PARQUET_FILE,
    columns=["VendorID", "passenger_count", "trip_distance", "fare_amount"]
)
df = df.sample(n=SIZE).copy()

if STREAM_MODE == "LOW":
    df = df.head(100)

start_time = datetime.now() + timedelta(seconds=5)
timestamps = []
current_time = start_time

for i in range(len(df)):
    if STREAM_MODE == "LOW":
        interval = 5
    elif STREAM_MODE == "NORMAL":
        interval = 1
    elif STREAM_MODE == "HIGH":
        interval = 0.2
    elif STREAM_MODE == "BURST":
        interval = 5 if i % 10 == 0 else 0
    elif STREAM_MODE == "RANDOM":
        interval = random.uniform(0.5, 2)
    else:
        interval = 1
    current_time += timedelta(seconds=interval)
    timestamps.append(current_time)

df["sim_timestamp"] = timestamps

print(f"Loaded {len(df)} records")
print(f"Running in {STREAM_MODE} mode")
print(f"Simulation starts at: {df['sim_timestamp'].iloc[0]}")
print(f"Simulation ends at:   {df['sim_timestamp'].iloc[-1]}")

for index, row in df.iterrows():
    target_time = row["sim_timestamp"].to_pydatetime()
    now = datetime.now()
    if target_time > now:
        time.sleep((target_time - now).total_seconds())

    record = {
        "VendorID": int(row["VendorID"]) if not pd.isna(row["VendorID"]) else None,
        "passenger_count": int(row["passenger_count"]) if not pd.isna(row["passenger_count"]) else None,
        "trip_distance": float(row["trip_distance"]) if not pd.isna(row["trip_distance"]) else None,
        "fare_amount": float(row["fare_amount"]) if not pd.isna(row["fare_amount"]) else None,
        "event_time": target_time.strftime("%Y-%m-%d %H:%M:%S"),
        "ingest_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stream_mode": STREAM_MODE
    }

    try:
        producer.send(TOPIC, record)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent record {index}")
    except Exception as e:
        print(f"Failed to send record {index}: {e}")

producer.flush()
producer.close()
print("Streaming simulation complete.")
