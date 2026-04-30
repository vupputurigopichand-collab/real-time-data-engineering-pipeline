import json
import time
import pandas as pd
from kafka import KafkaProducer

# Kafka configuration
KAFKA_TOPIC = "taxi_events"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Load sample data
df = pd.read_csv("../data/sample_taxi_data.csv")

print("Starting data stream...")

for _, row in df.iterrows():
    event = row.to_dict()
    producer.send(KAFKA_TOPIC, event)
    print("Sent:", event)
    time.sleep(1)  # simulate real-time delay

producer.flush()
producer.close()

