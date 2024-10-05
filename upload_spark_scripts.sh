#!/bin/bash

kubectl delete job.batch/copy-spark-script
kubectl delete configmap spark-script-config

kubectl create configmap spark-script-config --from-file=/home/barryma/Workspace/tasks/kubernetes/examen-db/src/spark/scripts/ --namespace data-pipeline

kubectl apply -f src/spark/spark-script-copy-job.yaml
