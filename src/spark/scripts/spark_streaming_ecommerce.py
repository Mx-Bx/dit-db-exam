import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, MapType, BooleanType
from pyspark.sql.functions import from_json, col, date_format

# Logging Configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

# Environment Variables for Configuration
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka.data-pipeline.svc.cluster.local:9092")
PRODUCT_TOPIC = os.getenv("PRODUCT_TOPIC", "products")
CUSTOMER_TOPIC = os.getenv("CUSTOMER_TOPIC", "customers")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "orders")
USER_ACTIVITY_TOPIC = os.getenv("USER_ACTIVITY_TOPIC", "user_activity")

CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "ecommerce")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/mnt/data/check_point")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra.data-pipeline.svc.cluster.local")

logger.info(f"Kafka Server: {KAFKA_SERVER}")
logger.info(f"Cassandra Host: {CASSANDRA_HOST}")
logger.info(f"Cassandra Keyspace: {CASSANDRA_KEYSPACE}")

def spark_process():
    # Initialize Spark Session with Cassandra Configuration
    spark = SparkSession \
        .builder \
        .appName("EcommerceSparkStreaming") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()

    # Define Schemas for Each Kafka Topic
    product_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("availability", BooleanType(), False),
        StructField("supplier", StringType(), False)
    ])

    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("address", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("created_at", StringType(), False)
    ])

    order_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", StringType(), False),
        StructField("status", StringType(), False),
        StructField("items", MapType(StringType(), StringType()), False),
        StructField("total_amount", FloatType(), False),
        StructField("bucket_month", StringType(), False)
    ])

    user_activity_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("bucket_day", StringType(), False),
        StructField("interaction_time", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), False)
    ])

    # Create Streaming DataFrames for Each Kafka Topic
    def kafka_stream(topic, schema):
        return spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")

    product_df = kafka_stream(PRODUCT_TOPIC, product_schema)
    customer_df = kafka_stream(CUSTOMER_TOPIC, customer_schema)
    order_df = kafka_stream(ORDER_TOPIC, order_schema)
    user_activity_df = kafka_stream(USER_ACTIVITY_TOPIC, user_activity_schema)

    # Fix Timestamp Columns for Cassandra Compatibility
    customer_df = customer_df.withColumn("created_at", date_format(col("created_at"), "yyyy-MM-dd HH:mm:ss.SSS"))
    order_df = order_df.withColumn("order_date", date_format(col("order_date"), "yyyy-MM-dd HH:mm:ss.SSS"))
    user_activity_df = user_activity_df.withColumn("interaction_time", date_format(col("interaction_time"), "yyyy-MM-dd HH:mm:ss.SSS"))

    # Write Each DataFrame to Corresponding Cassandra Table
    def write_to_cassandra(df, table_name):
        return df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .outputMode("append") \
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/{table_name}") \
            .option("keyspace", CASSANDRA_KEYSPACE) \
            .option("table", table_name) \
            .start()

    # Write Each DataFrame to Its Corresponding Cassandra Table
    product_query = write_to_cassandra(product_df, "products")
    customer_query = write_to_cassandra(customer_df, "customers")
    order_query = write_to_cassandra(order_df, "order_history")
    user_activity_query = write_to_cassandra(user_activity_df, "customer_behavior")

    # Wait for All Queries to Complete
    product_query.awaitTermination()
    customer_query.awaitTermination()
    order_query.awaitTermination()
    user_activity_query.awaitTermination()


if __name__ == '__main__':
    spark_process()
