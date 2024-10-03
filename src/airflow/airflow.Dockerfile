# Base Image for Airflow
FROM apache/airflow:2.4.2-python3.10

# Switch to root user for installations
USER root

# Install OpenJDK-11 for Java dependencies (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean

# Revert back to airflow user
USER airflow

# Copy the requirements file and install Python dependencies
COPY ./requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

# Define the Healthcheck for Kubernetes
HEALTHCHECK CMD curl --fail http://localhost:8080/health || exit 1

