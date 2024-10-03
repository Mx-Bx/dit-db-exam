import time
import os
from faker import Faker
from kafka import KafkaProducer
import json

# Initialize Faker
fake = Faker()

# Environment Variables for Kafka Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fake_data")  # Default topic: fake_data
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka.data-pipeline.svc.cluster.local:9092")

# Function to Generate Fake Data
def generate_kafka_data() -> dict:
    kafka_data = {
        "name": fake.name(),
        "company": fake.company(),
        "remote_ip": fake.ipv4_public(),
        "user_agent": fake.user_agent(),
        "date": str(fake.date_between("today", "+8h"))
    }
    return kafka_data

# Kafka Producer Setup
def kafka_producer():
    return KafkaProducer(bootstrap_servers=[KAFKA_SERVER])

# Stream Data to Kafka
def stream_data():
    producer = kafka_producer()
    print(f"Connected to Kafka at {KAFKA_SERVER}, sending data to topic: {KAFKA_TOPIC}")

    end_time = time.time() + 120  # Stream for 2 minutes
    while True:
        if time.time() > end_time:
            break

        kafka_data = generate_kafka_data()
        producer.send(KAFKA_TOPIC, json.dumps(kafka_data).encode('utf-8'))
        print(f"Produced data: {kafka_data}")
        time.sleep(5)

if __name__ == "__main__":
    stream_data()

