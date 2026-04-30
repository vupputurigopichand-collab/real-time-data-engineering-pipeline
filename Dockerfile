FROM apache/airflow:2.8.1

USER root

# Install Java + curl
RUN apt-get update && apt-get install -y openjdk-17-jdk curl && apt-get clean

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    && tar -xzf spark.tgz \
    && mv spark-3.5.0-bin-hadoop3 /opt/spark \
    && rm spark.tgz

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow

# Install Python dependencies
RUN pip install kafka-python pandas pyarrow