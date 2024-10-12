import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

# Logging Configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

# Environment Variables for Configuration
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka.data-pipeline.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fake_data")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "spark_streaming")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "fake_person_table")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra.data-pipeline.svc.cluster.local")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/mnt/data/check_point")
#CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/opt/bitnami/spark/check_point")

logger.info(f"Kafka Server: {KAFKA_SERVER}")
logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
logger.info(f"Cassandra Host: {CASSANDRA_HOST}")
logger.info(f"Cassandra Keyspace: {CASSANDRA_KEYSPACE}")
logger.info(f"Cassandra Table: {CASSANDRA_TABLE}")

def spark_process():
    # Initialize Spark Session with Cassandra Configuration
    spark = SparkSession \
        .builder \
        .appName("SparkStreamingToCassandra") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()

    # Define Schema for Incoming Data
    schema = StructType([
        StructField("name", StringType(), False),
        StructField("company", StringType(), False),
        StructField("remote_ip", StringType(), False),
        StructField("user_agent", StringType(), False),
        StructField("date", StringType(), False)
    ])

    # Read Data Stream from Kafka
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    logger.info("Successfully connected to Kafka and started reading the stream.")

    # Convert Kafka Value to Structured Data
    df_final = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                     .select("data.remote_ip", "data.name", "data.company", "data.user_agent", "data.date")

    # Write Stream to Cassandra
    query = df_final.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode("append") \
        .option("table", CASSANDRA_TABLE) \
        .option("keyspace", CASSANDRA_KEYSPACE) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    logger.info("Started streaming data to Cassandra.")

    query.awaitTermination()


if __name__ == '__main__':
    spark_process()

