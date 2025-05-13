# Usa una versione stabile e compatibile di Spark (Scala 2.12)
FROM bitnami/spark:3.5.0

# Imposta la directory di lavoro
WORKDIR /app

# Copia il codice del consumer
COPY src/delta/bronze/sensors_consumers/ .

# Installa le dipendenze Python necessarie
RUN pip install --no-cache-dir psycopg2-binary bcrypt boto3

# Scarica i connettori Kafka per Spark Structured Streaming
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -P /opt/bitnami/spark/jars/ \
 && wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar -P /opt/bitnami/spark/jars/

# Comando di avvio del tuo consumer Spark
CMD ["spark-submit", "--master", "local[*]", "bpm_consumer.py"]
