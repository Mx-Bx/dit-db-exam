from datetime import datetime
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator  # Updated import for PythonOperator
from airflow.operators.empty import EmptyOperator
import os

# Import the data generation function
from generate_data_to_kafka import stream_data

# Environment Variables (optional, in case needed)
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka.data-pipeline.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fake_data")

# Define the DAG using the Airflow Decorator
@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 2),
    catchup=False,
    dag_id='generate_random_data_k8s'
)
def generate_random_data() -> None:
    # Start Task
    start = EmptyOperator(task_id='start')

    # Stream Data Task
    stream_task = PythonOperator(
        task_id='fake_data_streaming_kafka',
        python_callable=stream_data
    )

    # End Task
    end = EmptyOperator(task_id='end')

    # Set Task Dependencies
    start >> stream_task >> end

# Generate the DAG
dag = generate_random_data()
