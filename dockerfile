# Partiamo dalla stessa immagine ufficiale di Airflow
FROM apache/airflow:2.9.2

# Definiamo le versioni come argomenti
ARG SPARK_VERSION="3.5.6"
ARG HADOOP_VERSION="3"

# Passiamo a 'root' per installare pacchetti
USER root

# Installiamo JDK e wget
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Impostiamo JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Installiamo la distribuzione completa di Spark
RUN wget -O /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# Impostiamo SPARK_HOME e PATH
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# --- NUOVA AGGIUNTA ---
# Creiamo una cartella per i JAR di S3 e li scarichiamo
RUN mkdir -p /opt/spark/connectors && \
    wget -O /opt/spark/connectors/hadoop-aws-3.3.4.jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    wget -O /opt/spark/connectors/aws-java-sdk-bundle-1.12.262.jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Torniamo all'utente 'airflow'
USER airflow

# Installiamo il provider e pyspark
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==4.1.2" \
    "pyspark==3.5.6" \
    --timeout 600