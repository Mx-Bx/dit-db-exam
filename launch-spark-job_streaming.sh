#!/bin/bash

/opt/spark/bin/spark-submit --master k8s://https://127.0.0.1:34803 \
  --deploy-mode cluster \
  --conf spark.executor.instances=1 \
  --conf spark.kubernetes.namespace=data-pipeline \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=spark:spark-kb8 \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.single-access-pvc.mount.path=/mnt/data \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.single-access-pvc.mount.readOnly=false \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.single-access-pvc.options.claimName=single-access-pvc \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.single-access-pvc.mount.path=/mnt/data \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.single-access-pvc.mount.readOnly=false \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.single-access-pvc.options.claimName=single-access-pvc \
  --conf spark.kubernetes.driver.volumes.emptyDir.ivy-cache.mount.path=/opt/spark/.ivy2 \
  --conf spark.kubernetes.driver.volumes.emptyDir.ivy-cache.mount.readOnly=false \
  --conf spark.kubernetes.executor.volumes.emptyDir.ivy-cache.mount.path=/opt/spark/.ivy2 \
  --conf spark.kubernetes.executor.volumes.emptyDir.ivy-cache.mount.readOnly=false \
  --conf spark.cassandra.auth.username="cassandra" \
  --conf spark.cassandra.auth.password="cassandra" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,com.github.jnr:jnr-posix:3.1.15 \
  local:///mnt/data/spark-data/upload-dir/spark_streaming_ecommerce.py
