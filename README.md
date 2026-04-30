# Real-Time Data Engineering Pipeline
## COMP1682 Final Year Project — University of Greenwich
**Student:** Gopi Chand Vupputuri | **SID:** 001345369 | **Supervisor:** Dr. Stef Garasto | **April 2026**

---

## Project Overview

This project builds a complete real-time data engineering pipeline using historical NYC Taxi trip data to simulate live streaming. The pipeline uses Apache Kafka for message streaming, Apache Spark for real-time processing, Apache Airflow for orchestration, MySQL for storage, and Streamlit for visualisation — all containerised with Docker.

---

## Prerequisites

Install these before running:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — must be running
- Python 3.12+
- pip

---

## Data File

The NYC Taxi dataset (1.2GB) is too large to include in this submission.

**Download the data file here:**
> [Download yellow_tripdata_2019-02.parquet](https://drive.google.com/file/d/11crL-CvyFW1zbdvJ-ej-_Nb0ED45AMJj/view?usp=sharing)

After downloading, place it at:
```
airflow/data/raw/yellow/2019/yellow_tripdata_2019-02.parquet
```

Create the folders if they don't exist:
```bash
mkdir -p airflow/data/raw/yellow/2019/
```

> **Note:** Any month's NYC Yellow Taxi parquet file can be used to verify the pipeline runs correctly. Data is available from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) website. Just rename the file to `yellow_tripdata_2019-02.parquet` and place it in the folder above.

---

## Setup & Run

### Step 1 — Start all containers:
```bash
docker compose up -d
```
Wait 60 seconds for all services to start.

### Step 2 — Verify containers are running:
```bash
docker ps
```
You should see: `airflow-webserver`, `airflow-scheduler`, `airflow-worker`, `kafka`, `mysql`

> **Note:** Spark runs inside the airflow-worker container — there is no separate Spark container.

### Step 3 — Connect Kafka to Airflow network:
```bash
docker network connect airflow_default kafka
```

### Step 4 — Create Kafka topic:
```bash
docker exec -it kafka kafka-topics --create \
  --topic yellow_taxi \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### Step 5 — Trigger the pipeline:
```
Open: http://localhost:8080
Username: airflow
Password: airflow
Find taxi_streaming_pipeline → click ▶ to trigger
```

### Step 6 — Start the dashboard:
```bash
pip install streamlit pandas plotly pymysql
cd dashboard
python3 -m streamlit run app.py
```
```
Open: http://localhost:8501
```

---

## Changing Stream Mode

Edit this file:
```
airflow/scripts/kafka_producer_yellow.py
```

Change this line:
```python
STREAM_MODE = "NORMAL"  # Options: HIGH, NORMAL, LOW, BURST
```

---

## Reset Between Runs

Run this before each new scenario:
```bash
docker exec -it airflow-airflow-worker-1 bash -c "rm -rf /opt/airflow/checkpoints" && \
docker restart kafka && \
sleep 20 && \
docker network connect airflow_default kafka 2>/dev/null || true && \
sleep 5 && \
docker exec -it kafka kafka-topics --create --topic yellow_taxi \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 && \
docker exec -it mysql mysql -u root -proot taxi_db -e \
  "TRUNCATE TABLE clean_taxi_trips; TRUNCATE TABLE vendor_revenue_summary; \
   TRUNCATE TABLE vendor_trip_stats; TRUNCATE TABLE passenger_demand;"
```

---

## Check Results

```bash
docker exec -it mysql mysql -u root -proot taxi_db -e "
SELECT stream_mode, COUNT(*) as records,
ROUND(AVG(TIMESTAMPDIFF(SECOND, event_time, inserted_at)),2) as avg_latency,
MIN(TIMESTAMPDIFF(SECOND, event_time, inserted_at)) as min_latency,
MAX(TIMESTAMPDIFF(SECOND, event_time, inserted_at)) as max_latency
FROM clean_taxi_trips GROUP BY stream_mode;"
```

---

## Project Structure

```
001345369-FYP_Code/
├── airflow/
│   ├── dags/
│   │   └── taxi_pipeline_dag.py          # Airflow DAG — orchestrates the pipeline
│   ├── scripts/
│   │   └── kafka_producer_yellow.py      # Kafka producer — change STREAM_MODE here
│   └── spark/
│       └── spark_stream_kafka.py         # Spark streaming job
├── dashboard/
│   └── app.py                            # Streamlit dashboard
├── docker-compose.yml                    # All services defined here
└── README.md
```

---

## Services & Ports

| Service       | URL / Port             |
|--------------|------------------------|
| Airflow UI   | http://localhost:8080  |
| Dashboard    | http://localhost:8501  |
| MySQL        | localhost:3307         |
| Kafka        | localhost:9092         |

---

## Tech Stack

| Component     | Technology         | Version |
|--------------|--------------------|---------|
| Orchestration | Apache Airflow     | 2.9     |
| Streaming     | Apache Kafka       | 3.4     |
| Processing    | Apache Spark       | 3.5     |
| Storage       | MySQL              | 8.0     |
| Dashboard     | Streamlit          | Latest  |
| Containers    | Docker             | Latest  |

---

## Evaluation Results of run 1 of 5 runs

| Scenario | Records | Avg Latency | Min | Max |
|----------|---------|-------------|-----|-----|
| HIGH     | 982     | 1.27s       | 0s  | 5s  |
| NORMAL   | 970     | 1.08s       | 1s  | 6s  |
| LOW      | 96      | 2.01s       | 2s  | 5s  |
| BURST    | 977     | 1.24s       | 1s  | 5s  |

**Fault Tolerance:** 13 second recovery time, 0 records lost

**Scalability:**

| Partitions    | Records | Avg Latency |
|--------------|---------|-------------|
| 1 (baseline) | 965     | 1.15s       |
| 2            | 974     | 1.47s       |
| 4            | 966     | 1.70s       |

---

## Troubleshooting

**Kafka topic already exists:**
```bash
docker restart kafka
sleep 20
docker network connect airflow_default kafka 2>/dev/null || true
```

**Dashboard shows no data:**
- Wait 1-2 minutes after triggering DAG
- Refresh the dashboard page

**MySQL tables missing:**
- Tables are created automatically by Spark
- Just trigger the DAG and wait

---

*University of Greenwich | COMP1682 | BSc (Hons) Computer Science | April 2026*
