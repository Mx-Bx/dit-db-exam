# Real-Time Data Pipeline: Kafka to Cassandra using Spark and Airflow in Kubernetes

## Introduction

This project demonstrates a real-time data pipeline that generates fake person data using the `faker` Python package and sends it to Apache Kafka. We orchestrate this process with Apache Airflow. The data is consumed via Spark Structured Streaming and written to Cassandra. All services are deployed using Kubernetes.

## Tech Stack

- **Python**
- **Apache Airflow**
- **Apache Kafka**
- **Apache Spark**
- **Apache Cassandra**
- **Kubernetes**

## Project Structure

1. **Data Generation**: Uses `faker` to generate synthetic data and sends it to a Kafka topic.
2. **Data Orchestration**: Airflow DAGs manage the data generation schedule.
3. **Data Streaming**: Spark Structured Streaming consumes data from Kafka.
4. **Data Storage**: Processed data is stored in Cassandra.

## Kubernetes Setup Instructions

### 1. Prerequisites

- A Kubernetes cluster (local or cloud-based).
- `kubectl` installed and configured to interact with your cluster.
- Helm (optional) for managing applications in Kubernetes.

### 2. Create a Namespace

First, create a dedicated namespace for this project:

```bash
kubectl apply -f namespace.yaml

kubectl apply -f pod-access-role.yaml

kubectl apply -f pod-exec-role.yaml

kubectl apply -f pod-exec-rolebinding.yaml

kubectl apply -f spark-role.yaml

kubectl apply -f spark-rolebinding.yaml

./launch-spark-job.sh


mkdir -p /opt/spark/check_point  && chmod 777 /opt/spark/check_point
```

This will create a `data-pipeline` namespace where all services will run.

### 3. Deploy PostgreSQL and Redis

Deploy the PostgreSQL and Redis services, which are required for Airflow:

```bash
kubectl apply -f postgres.yaml
kubectl apply -f redis.yaml
```

### 4. Deploy Airflow

Build the custom airflow image:

```bash
docker build -t barryma22/airflow:2.4.2-python3.10 -f src/airflow/airflow.Dockerfile src/airflow
```

Apply the manifests for Airflow (Webserver, Scheduler, Worker, etc.):

```bash
kubectl apply -f deployment/airflow.yaml

kubectl create configmap airflow-scripts --from-file=/home/barryma/Workspace/tasks/kubernetes/examen-db/src/airflow/scripts/ --namespace data-pipeline

kubectl apply -f src/airflow-scripts-copy-job.yaml

kubectl exec -n data-pipeline airflow-webserver-78f56f7f4-2jdnn -- ls /opt/airflow/dags/
```

Ensure all pods are running:

```bash
kubectl get pods -n data-pipeline
```

Once Airflow is running, you can access the webserver at:

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-pipeline
```

Access it via your browser at `http://localhost:8080`.

### 5. Deploy Zookeeper and Kafka

Deploy Zookeeper and Kafka using:

```bash
kubectl apply -f deployment/kafka-zookeeper.yaml
```

### 6. Deploy Spark Master and Workers

Deploy the Spark Master and Workers:

```bash
kubectl apply -f deployment/spark.yaml
```

Verify the Spark UI is accessible:

```bash
kubectl port-forward svc/spark-master 8080:8080 -n data-pipeline
```

### 7. Deploy Cassandra

Deploy the Cassandra database:

```bash
kubectl apply -f deployment/cassandra.yaml
```

### 8. Verifying the Setup

1. **Airflow DAGs**: Check that DAGs are running successfully in Airflow.
2. **Kafka**: Produce and consume messages using Kafka.
3. **Spark**: Ensure Spark can process Kafka data and write to Cassandra.
4. **Cassandra**: Verify data is written correctly to Cassandra.

### 9. Demo

1. **Copying the Spark script to the master pod.**
2. **Creating the Cassandra keyspace and table.**
3. **Running the `spark-submit` command to start the streaming job.**

## Step 1: Create a Kubernetes Job to Copy the Spark Script

This job will use a temporary busybox container to copy the `spark_streaming.py` script into the Spark master pod using a Kubernetes command.

```bash
kubectl create configmap spark-script-config --from-file=src/scripts/spark_streaming.py --namespace data-pipeline

kubectl apply -f src/spark-script-copy-job.yaml
```

---

## Step 2: Create a Kubernetes Job to Set Up the Cassandra Schema

This job will connect to the Cassandra pod, run `cqlsh`, and create the keyspace and table.

```bash
kubectl apply -f src/cassandra-schema-setup-job.yaml
```

> This job uses `kubectl exec` to execute the Cassandra commands inside the pod. Adjust the schema setup as needed.

---

## Step 3: Create a Kubernetes Job for `spark-submit`

The job will run the `spark-submit` command in the Spark master pod to start the streaming application.

```bash
docker build -t barryma22/spark-k8s -f src/spark/spark.Dockerfile src/spark

docker push barryma22/spark-k8s

kubectl apply -f src/spark-submit-job.yaml
```

> The `spark-submit` job runs the script on the Spark master. Ensure the master service is configured properly (`spark://spark-master:7077`).

---

## Step 4: Automating Dependencies Using a Kubernetes CronJob

```bash
chmod +x run-all-jobs.sh
./run-all-jobs.sh
```

---

## Final Considerations

1. **Check Kubernetes Job Status**: Use `kubectl get jobs -n data-pipeline` to see the status of each job.
2. **Debugging Logs**: Use `kubectl logs job/<job-name> -n data-pipeline` to view the logs of each job.
3. **Adjust Timings**: You might need to adjust sleep durations (`sleep 10` etc.) depending on the readiness of each service.

### 10. Cleanup

To clean up the entire project, you can delete the namespace:

```bash
kubectl delete namespace data-pipeline
```
