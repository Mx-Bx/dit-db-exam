#!/bin/bash

kubectl delete job.batch/copy-airflow-scripts
kubectl delete configmap airflow-scripts

kubectl create configmap airflow-scripts --from-file=/home/barryma/Workspace/tasks/kubernetes/examen-db/src/airflow/scripts/ --namespace data-pipeline

kubectl apply -f src/airflow/airflow-scripts-copy-job.yaml
