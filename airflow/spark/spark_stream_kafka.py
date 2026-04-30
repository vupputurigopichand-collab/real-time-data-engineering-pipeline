from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, count, max, min, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
import time

spark = SparkSession.builder \
    .appName("YellowTaxiKafkaStream") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("stream_mode", StringType(), True),
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "yellow_taxi") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")

clean_df = parsed_df.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0) &
    (col("VendorID").isNotNull()) &
    (col("passenger_count") > 0)
)

JDBC_URL = "jdbc:mysql://mysql:3306/taxi_db"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_USER = "root"
JDBC_PASSWORD = "root"

def write_silver(batch_df, batch_id):
    try:
        if batch_df.rdd.isEmpty():
            return
        batch_df.withColumn("inserted_at", current_timestamp()) \
            .write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("driver", JDBC_DRIVER) \
            .option("dbtable", "clean_taxi_trips") \
            .option("user", JDBC_USER) \
            .option("password", JDBC_PASSWORD) \
            .mode("append") \
            .save()
        print(f"Silver batch {batch_id} written")

        vendor_revenue = batch_df.groupBy("VendorID").agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
            sum("fare_amount").alias("total_revenue"),
            max("fare_amount").alias("max_fare"),
            min("fare_amount").alias("min_fare")
        )
        vendor_revenue.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("driver", JDBC_DRIVER) \
            .option("dbtable", "vendor_revenue_summary") \
            .option("user", JDBC_USER) \
            .option("password", JDBC_PASSWORD) \
            .mode("overwrite") \
            .save()
        print(f"vendor_revenue_summary written")

        vendor_stats = batch_df.groupBy("VendorID").agg(
            avg("trip_distance").alias("avg_distance"),
            max("trip_distance").alias("max_distance"),
            min("trip_distance").alias("min_distance"),
            avg("passenger_count").alias("avg_passengers")
        )
        vendor_stats.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("driver", JDBC_DRIVER) \
            .option("dbtable", "vendor_trip_stats") \
            .option("user", JDBC_USER) \
            .option("password", JDBC_PASSWORD) \
            .mode("overwrite") \
            .save()
        print(f"vendor_trip_stats written")

        passenger = batch_df.groupBy("passenger_count").agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare")
        )
        passenger.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("driver", JDBC_DRIVER) \
            .option("dbtable", "passenger_demand") \
            .option("user", JDBC_USER) \
            .option("password", JDBC_PASSWORD) \
            .mode("overwrite") \
            .save()
        print(f"passenger_demand written")

    except Exception as e:
        print(f"ERROR in batch {batch_id}: {e}")

silver_query = clean_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_silver) \
    .option("checkpointLocation", f"/opt/airflow/checkpoints/silver_{int(time.time())}") \
    .start()

spark.streams.awaitAnyTermination(timeout=1100)
