import pandas as pd
from datetime import datetime, timedelta
import os

PARQUET_FILE = "/opt/airflow/data/raw/yellow/2019/yellow_tripdata_2019-01.parquet"
OUTPUT_FILE = "/opt/airflow/data/raw/yellow/2019/yellow_tripdata_simulation.parquet"

print("Reading parquet file...")
df = pd.read_parquet(
    PARQUET_FILE,
    columns=["VendorID", "passenger_count", "trip_distance", "fare_amount", "tpep_pickup_datetime"]
)

# Take 100 records
df = df.head(100).copy()

# Start time = 2 minutes from now
start_time = datetime.now() + timedelta(minutes=2)

# Space records 5 seconds apart
df["sim_timestamp"] = [
    start_time + timedelta(seconds=i * 5)
    for i in range(len(df))
]

print(f"First record will arrive at: {df['sim_timestamp'].iloc[0]}")
print(f"Last record will arrive at:  {df['sim_timestamp'].iloc[-1]}")
print(f"Total simulation time: ~{len(df) * 5 / 60:.1f} minutes")

df.to_parquet(OUTPUT_FILE, index=False)
print(f"Saved simulation file to {OUTPUT_FILE}")
