import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Import data generation functions from the updated `ecommerce_data.py`
from ecommerce_data import generate_products, generate_customers, generate_orders, generate_user_activity

# Define paths for storing the generated data (temporary files)
PRODUCTS_FILE = "/tmp/products.json"
CUSTOMERS_FILE = "/tmp/customers.json"

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

    # Task to Generate and Save Product Data
    def generate_and_save_products(**kwargs):
        products = generate_products(30)  # Number of products to generate
        with open(PRODUCTS_FILE, 'w') as f:
            json.dump(products, f)
        print(f"Products saved to {PRODUCTS_FILE}")

    stream_products_task = PythonOperator(
        task_id='stream_products_to_kafka',
        python_callable=generate_and_save_products,
        provide_context=True,
    )

    # Task to Generate and Save Customer Data
    def generate_and_save_customers(**kwargs):
        customers = generate_customers(100)  # Number of customers to generate
        with open(CUSTOMERS_FILE, 'w') as f:
            json.dump(customers, f)
        print(f"Customers saved to {CUSTOMERS_FILE}")

    stream_customers_task = PythonOperator(
        task_id='stream_customers_to_kafka',
        python_callable=generate_and_save_customers,
        provide_context=True,
    )

    # Task to Generate Orders Using Saved Products and Customers
    def generate_and_stream_orders(**kwargs):
        with open(PRODUCTS_FILE, 'r') as f:
            products = json.load(f)
        with open(CUSTOMERS_FILE, 'r') as f:
            customers = json.load(f)
        generate_orders(customers, products, 200)

    stream_orders_task = PythonOperator(
        task_id='stream_orders_to_kafka',
        python_callable=generate_and_stream_orders,
        provide_context=True,
    )

    # Task to Generate User Activity Logs Using Saved Customers
    def generate_and_stream_user_activity(**kwargs):
        with open(CUSTOMERS_FILE, 'r') as f:
            customers = json.load(f)
        generate_user_activity(customers, 500)

    stream_user_activity_task = PythonOperator(
        task_id='stream_user_activity_to_kafka',
        python_callable=generate_and_stream_user_activity,
        provide_context=True,
    )

    # End Task
    end = EmptyOperator(task_id='end')

    # Task Dependencies
    start >> [stream_products_task, stream_customers_task] >> stream_orders_task
    stream_orders_task >> stream_user_activity_task >> end
