import os
import random
from datetime import datetime
from uuid import uuid4
from faker import Faker
from kafka import KafkaProducer
import json

# Configuration for Kafka (using environment variables)
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka.data-pipeline.svc.cluster.local:9092")
PRODUCT_TOPIC = os.getenv("PRODUCT_TOPIC", "products")
CUSTOMER_TOPIC = os.getenv("CUSTOMER_TOPIC", "customers")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "orders")
USER_ACTIVITY_TOPIC = os.getenv("USER_ACTIVITY_TOPIC", "user_activity")

# Initialize Faker
fake = Faker()

# Kafka Producer Setup
def kafka_producer():
    return KafkaProducer(bootstrap_servers=[KAFKA_SERVER])

# Function to Stream Data to Kafka
def stream_to_kafka(topic, data):
    producer = kafka_producer()
    producer.send(topic, json.dumps(data).encode('utf-8'))
    print(f"Data sent to topic {topic}: {data}")

# Data Generation Functions without Cassandra Insertion
def generate_products(n=20):
    categories = ['smartphones', 'laptops', 'headphones', 'smartwatches']
    products = []
    for _ in range(n):
        product_data = {
            "product_id": str(uuid4()),
            "name": fake.word() + " " + fake.color_name(),
            "category": random.choice(categories),
            "price": round(random.uniform(50.0, 1000.0), 2),
            "availability": random.choice([True, False]),
            "supplier": fake.company()
        }
        products.append(product_data)
        stream_to_kafka(PRODUCT_TOPIC, product_data)
    return products

def generate_customers(n=50):
    customers = []
    for _ in range(n):
        customer_data = {
            "customer_id": str(uuid4()),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "address": fake.address(),
            "phone": fake.phone_number(),
            "created_at": str(datetime.now())
        }
        customers.append(customer_data)
        stream_to_kafka(CUSTOMER_TOPIC, customer_data)
    return customers

def generate_orders(customers, products, n=100):
    orders = []
    for _ in range(n):
        customer = random.choice(customers)
        product = random.choice(products)
        order_data = {
            "order_id": str(uuid4()),
            "customer_id": customer["customer_id"],
            "order_date": str(datetime.now()),
            "status": random.choice(['pending', 'shipped', 'delivered', 'cancelled']),
            "items": {product['product_id']: random.randint(1, 5)},
            "total_amount": product['price'],
            "bucket_month": datetime.now().strftime("%Y-%m")
        }
        orders.append(order_data)
        stream_to_kafka(ORDER_TOPIC, order_data)
    return orders

def generate_user_activity(customers, n=200):
    activities = []
    event_types = ['login', 'view_product', 'add_to_cart', 'purchase', 'logout']
    for _ in range(n):
        customer = random.choice(customers)
        activity_data = {
            "customer_id": customer["customer_id"],
            "bucket_day": str(datetime.now().date()),
            "interaction_time": str(datetime.now()),
            "session_id": str(uuid4()),
            "event_type": random.choice(event_types),
            "product_id": str(uuid4())
        }
        activities.append(activity_data)
        stream_to_kafka(USER_ACTIVITY_TOPIC, activity_data)
    return activities

# Main Function to Generate and Stream Data
if __name__ == "__main__":
    print(f"Connecting to Kafka at {KAFKA_SERVER}...")

    # Generate and Stream E-commerce Data
    products = generate_products(30)
    customers = generate_customers(100)
    generate_orders(customers, products, 200)
    generate_user_activity(customers, 500)

    print("Data generation and Kafka streaming completed.")
