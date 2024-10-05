# Base image: Use a lightweight base like Ubuntu
FROM ubuntu:20.04

# Install necessary packages including kubectl
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    bash \
    wget \
    apt-transport-https \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Set up environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Download and install Apache Spark (without Hadoop)
ARG SPARK_VERSION=3.1.3
#ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz
ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz

RUN mkdir -p /opt/spark && \
    curl -fSL ${SPARK_URL} -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

# Set up environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download Kafka and Cassandra connectors
RUN apt-get update && apt-get install -y wget python3 python3-pip
# Install kubectl
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg && \
    chmod 644 /etc/apt/keyrings/kubernetes-apt-keyring.gpg && \
    echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /' | tee /etc/apt/sources.list.d/kubernetes.list && \
    chmod 644 /etc/apt/sources.list.d/kubernetes.list && \
    apt-get update && apt-get install -y kubectl
    
# Download and add the Log4J library
# wget -P /opt/spark/jars https://repo1.maven.org/maven2/log4j/log4j/1.2.17/log4j-1.2.17.jar && \
RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.3/spark-sql-kafka-0-10_2.12-3.1.3.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.1.0/spark-cassandra-connector-assembly_2.12-3.1.0.jar && \
    # wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.1.0/spark-cassandra-connector_2.12-3.1.0.jar && \
    # wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/datastax/cassandra/cassandra-driver-core/3.11.3/cassandra-driver-core-3.11.3.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/datastax/oss/native-protocol/1.5.0/native-protocol-1.5.0.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core-shaded/4.12.0/java-driver-core-shaded-4.12.0.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/datastax/oss/java-driver-query-builder/4.12.0/java-driver-query-builder-4.12.0.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/datastax/oss/java-driver-mapper-runtime/4.12.0/java-driver-mapper-runtime-4.12.0.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/datastax/oss/java-driver-shaded-guava/25.1-jre-graal-sub-1/java-driver-shaded-guava-25.1-jre-graal-sub-1.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/io/dropwizard/metrics/metrics-core/4.1.18/metrics-core-4.1.18.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.8.2/snappy-java-1.1.8.2.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.14.1/log4j-api-2.14.1.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.14.1/log4j-core-2.14.1.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.6.0/kafka-clients-2.6.0.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.8.0/commons-pool2-2.8.0.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.0-preview2/spark-token-provider-kafka-0-10_2.12-3.0.0-preview2.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/github/jnr/jnr-posix/3.1.11/jnr-posix-3.1.11.jar


# Set working directory
WORKDIR /opt/spark

# Copy the spark_streaming.py script into the image
COPY spark_streaming.py /opt/spark/

# Set entrypoint
ENTRYPOINT ["/bin/bash"]
