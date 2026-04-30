from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "binnu",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="taxi_streaming_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["streaming", "kafka", "spark"],
    description="Real-time taxi data pipeline using Kafka, Spark and MySQL",
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    prepare_data = BashOperator(
        task_id="prepare_simulation_data",
        bash_command="python3 -u /opt/airflow/scripts/prepare_timestamps.py",
        retries=1,
    )

    spark_stream = BashOperator(
    task_id="start_spark_stream",
    bash_command=(
        "/opt/spark/bin/spark-submit " 
        "--master local[*] "
        "--driver-memory 4g "
        "--executor-memory 4g "
        "--conf spark.sql.shuffle.partitions=2 "
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "mysql:mysql-connector-java:8.0.33 "
        "/opt/airflow/spark/spark_stream_kafka.py"
    ),
    execution_timeout=timedelta(minutes=20),
    retries=0,
)
    
    

    kafka_producer = BashOperator(
        task_id="start_kafka_producer",
        bash_command="sleep 45 && python3 -u /opt/airflow/scripts/kafka_producer_yellow.py",
        retries=0,
        retry_delay=timedelta(seconds=10),
    )

    start >> prepare_data >> [spark_stream, kafka_producer] >> end
