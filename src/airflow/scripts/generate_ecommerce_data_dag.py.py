import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Import data generation functions from the updated generate_ecommerce_data.py
from generate_ecommerce_data import generate_products, generate_customers, generate_orders, generate_user_activity

# Environment Variables for Kafka Configuration (Optional, depending on your needs)
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka.data-pipeline.svc.cluster.local:9092")
PRODUCT_TOPIC = os.getenv("PRODUCT_TOPIC", "products")
CUSTOMER_TOPIC = os.getenv("CUSTOMER_TOPIC", "customers")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "orders")
USER_ACTIVITY_TOPIC = os.getenv("USER_ACTIVITY_TOPIC", "user_activity")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 2),
    'retries': 1,
}

# Define the DAG
with DAG(
    'generate_ecommerce_data_kafka',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Start Task
    start = EmptyOperator(task_id='start')

    # Tasks to Stream Each Type of E-commerce Data
    stream_products_task = PythonOperator(
        task_id='stream_products_to_kafka',
        python_callable=generate_products,
        op_args=[30],  # Number of products to generate
        dag=dag
    )

    stream_customers_task = PythonOperator(
        task_id='stream_customers_to_kafka',
        python_callable=generate_customers,
        op_args=[100],  # Number of customers to generate
        dag=dag
    )

    # Wait until products and customers are streamed
    wait_for_initial_data = EmptyOperator(task_id='wait_for_initial_data')

    # Tasks for Orders and User Activity
    stream_orders_task = PythonOperator(
        task_id='stream_orders_to_kafka',
        python_callable=generate_orders,
        op_args=[None, None, 200],  # Customers and products are filled dynamically in `generate_orders`
        dag=dag
    )

    stream_user_activity_task = PythonOperator(
        task_id='stream_user_activity_to_kafka',
        python_callable=generate_user_activity,
        op_args=[None, 500],  # Customers will be passed dynamically for `generate_user_activity`
        dag=dag
    )

    # End Task
    end = EmptyOperator(task_id='end')

    # Task Dependencies
    start >> [stream_products_task, stream_customers_task] >> wait_for_initial_data
    wait_for_initial_data >> [stream_orders_task, stream_user_activity_task] >> end
