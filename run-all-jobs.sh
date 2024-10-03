#!/bin/bash

# Apply Spark script copy job
kubectl apply -f spark-script-copy-job.yaml
# Wait until the copy job is completed
kubectl wait --for=condition=complete job/copy-spark-script -n data-pipeline

# Apply Cassandra schema setup job
kubectl apply -f cassandra-schema-setup-job.yaml
# Wait until the schema setup job is completed
kubectl wait --for=condition=complete job/setup-cassandra-schema -n data-pipeline

# Apply the spark-submit job
kubectl apply -f spark-submit-job.yaml
# Wait until the spark-submit job is completed
kubectl wait --for=condition=complete job/spark-submit-job -n data-pipeline

echo "All jobs executed successfully!"

