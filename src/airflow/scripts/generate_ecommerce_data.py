import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from faker import Faker
from uuid import uuid4
from datetime import datetime, timedelta
import random

# Configuration du Cluster Cassandra à partir des variables d'environnement
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra.data-pipeline.svc.cluster.local")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", 9042)
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "ecommerce")
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "ecommerce_app_user")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "secure_password")

# Initialisation de Faker
fake = Faker()

# Connexion à Cassandra
auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
session = cluster.connect(CASSANDRA_KEYSPACE)

# Génération et Insertion des Données
def insert_data():
    products = generate_products(30)
    customers = generate_customers(100)
    generate_orders(customers, products, 200)
    generate_user_activity(customers, 500)

def generate_products(n=20):
    categories = ['smartphones', 'laptops', 'headphones', 'smartwatches']
    for _ in range(n):
        session.execute(
            """
            INSERT INTO products (product_id, name, category, price, availability, supplier)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, 
            (uuid4(), fake.word() + " " + fake.color_name(), random.choice(categories),
             round(random.uniform(50.0, 1000.0), 2), random.choice([True, False]), fake.company())
        )
    print(f"{n} produits insérés.")

def generate_customers(n=50):
    for _ in range(n):
        session.execute(
            """
            INSERT INTO customers (customer_id, first_name, last_name, email, address, phone, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (uuid4(), fake.first_name(), fake.last_name(), fake.email(), fake.address(), fake.phone_number(), datetime.now())
        )
    print(f"{n} clients insérés.")

def generate_orders(customers, products, n=100):
    for _ in range(n):
        customer = random.choice(customers)
        items = {random.choice(products)['product_id']: random.randint(1, 5)}
        total_amount = sum([products[0]['price'] for _ in items.values()])
        session.execute(
            """
            INSERT INTO order_history (order_id, customer_id, order_date, status, items, total_amount, bucket_month)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (uuid4(), customer['customer_id'], datetime.now(), random.choice(['pending', 'shipped', 'delivered', 'cancelled']),
             items, total_amount, datetime.now().strftime("%Y-%m"))
        )
    print(f"{n} commandes insérées.")

def generate_user_activity(customers, n=200):
    event_types = ['login', 'view_product', 'add_to_cart', 'purchase', 'logout']
    for _ in range(n):
        customer = random.choice(customers)
        session.execute(
            """
            INSERT INTO customer_behavior (customer_id, bucket_day, interaction_time, session_id, event_type, product_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (customer['customer_id'], datetime.now().date(), datetime.now(), uuid4(), random.choice(event_types), uuid4())
        )
    print(f"{n} logs d'activité générés.")
