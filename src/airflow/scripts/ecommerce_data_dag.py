import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

os.environ["CASSANDRA_HOST"] = "cassandra.data-pipeline.svc.cluster.local"
os.environ["CASSANDRA_PORT"] = "9042"
os.environ["CASSANDRA_USER"] = "cassandra"
os.environ["CASSANDRA_PASSWORD"] = "cassandra"

# Importer la fonction de génération de données
from generate_ecommerce_data import insert_data

# Environment Variables (optional, in case needed)
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka.data-pipeline.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fake_data")

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 2),
    'retries': 1,
}

# Définir le DAG
dag = DAG(
    'generate_cassandra_data',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Tâche d'Insertion des Données
generate_data_task = PythonOperator(
    task_id='insert_data_to_cassandra',
    python_callable=insert_data,
    dag=dag
)

# Définir les Dépendances
generate_data_task
