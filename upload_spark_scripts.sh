#!/bin/bash

#kubectl delete job.batch/copy-spark-script
#kubectl delete configmap spark-script-config

#kubectl create configmap spark-script-config --from-file=/home/barryma/Workspace/tasks/kubernetes/examen-db/src/spark/scripts/ --namespace data-pipeline
kubectl cp /home/barryma/Workspace/tasks/kubernetes/examen-db/src/spark/scripts/spark_streaming_ecommerce.py test-pvc-pod:/mnt/data/spark-data/upload-dir/spark_streaming_ecommerce.py

kubectl cp /home/barryma/Workspace/tasks/kubernetes/examen-db/src/spark/scripts/spark_streaming.py test-pvc-pod:/mnt/data/spark-data/upload-dir/spark_streaming.py


#kubectl apply -f src/spark/spark-script-copy-job.yaml
