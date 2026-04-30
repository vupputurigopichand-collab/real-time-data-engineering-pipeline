import os
import time
import json
import pandas as pd
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "events_topic"

# Base data path
BASE_DATA_PATH = "data/raw"

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def replay_parquet_files(dataset, start_year, end_year, delay=0.005):
    dataset_path = os.path.join(BASE_DATA_PATH, dataset)

    for year in range(start_year, end_year + 1):
        year_path = os.path.join(dataset_path, str(year))
        if not os.path.exists(year_path):
            print(f"Skipping missing year folder: {year_path}")
            continue

        for file in sorted(os.listdir(year_path)):
            if not file.endswith(".parquet"):
                continue

            file_path = os.path.join(year_path, file)
            print(f"📤 Replaying {file_path}")

            try:
                df = pd.read_parquet(file_path)
            except Exception as e:
                print(f"❌ Error reading {file}: {e}")
                continue

            for _, row in df.head(500).iterrows():
               #producer.send(TOPIC_NAME, row.to_dict())
                time.sleep(delay)

            print(f"✅ Finished {file}")

if __name__ == "__main__":
    replay_parquet_files(
        dataset="yellow",
        start_year=2019,
        end_year=2023
    )

