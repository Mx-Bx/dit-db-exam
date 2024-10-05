import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, DecimalType, TimestampType
from pyspark.sql.functions import from_json, col

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

# Variables d'environnement pour Kafka et Cassandra
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka.data-pipeline.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_data")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "ecommerce")
CASSANDRA_ORDER_TABLE = os.getenv("CASSANDRA_ORDER_TABLE", "order_history")
CASSANDRA_BEHAVIOR_TABLE = os.getenv("CASSANDRA_BEHAVIOR_TABLE", "customer_behavior")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra.data-pipeline.svc.cluster.local")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/opt/spark/check_point")

logger.info(f"Kafka Server: {KAFKA_SERVER}")
logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
logger.info(f"Cassandra Host: {CASSANDRA_HOST}")
logger.info(f"Cassandra Keyspace: {CASSANDRA_KEYSPACE}")
logger.info(f"Cassandra Order Table: {CASSANDRA_ORDER_TABLE}")
logger.info(f"Cassandra Behavior Table: {CASSANDRA_BEHAVIOR_TABLE}")

def spark_process():
    # Initialiser la session Spark avec les configurations Cassandra
    spark = SparkSession \
        .builder \
        .appName("ECommerceSparkStreaming") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()

    # Schéma pour les événements de commande (order_history)
    order_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("items", MapType(StringType(), StringType()), False),
        StructField("total_amount", DecimalType(), False)
    ])

    # Schéma pour les événements comportementaux des utilisateurs (customer_behavior)
    behavior_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("bucket_day", StringType(), False),
        StructField("interaction_time", TimestampType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), True)
    ])

    # Lire les flux de Kafka
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    logger.info("Connexion à Kafka réussie et début de lecture du flux.")

    # Transformer les données de commande pour correspondre au schéma `order_history`
    orders_df = raw_df.select(from_json(col("value").cast("string"), order_schema).alias("data")) \
                      .select("data.order_id", "data.customer_id", "data.order_date", "data.status", "data.items", "data.total_amount")

    # Écrire les commandes dans Cassandra
    order_query = orders_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode("append") \
        .option("table", CASSANDRA_ORDER_TABLE) \
        .option("keyspace", CASSANDRA_KEYSPACE) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    logger.info("Stream de données de commande vers Cassandra lancé.")

    # Transformer les données de comportement utilisateur pour correspondre au schéma `customer_behavior`
    behavior_df = raw_df.select(from_json(col("value").cast("string"), behavior_schema).alias("data")) \
                        .select("data.customer_id", "data.bucket_day", "data.interaction_time", "data.session_id", "data.event_type", "data.product_id")

    # Écrire les comportements des utilisateurs dans Cassandra
    behavior_query = behavior_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode("append") \
        .option("table", CASSANDRA_BEHAVIOR_TABLE) \
        .option("keyspace", CASSANDRA_KEYSPACE) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    logger.info("Stream de données comportementales vers Cassandra lancé.")

    # Attendre la terminaison des deux streams
    order_query.awaitTermination()
    behavior_query.awaitTermination()

if __name__ == '__main__':
    spark_process()
