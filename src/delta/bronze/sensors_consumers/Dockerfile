FROM bitnami/spark:3.5.0

USER root
WORKDIR /opt/bitnami/spark

# Scarica i JAR
RUN mkdir -p /opt/bitnami/spark/ext_jars
ADD https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar ext_jars/
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar ext_jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar ext_jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar ext_jars/

# ✅ FIX PERMESSI per Spark (user 1001)
RUN chown -R 1001:1001 /opt/bitnami/spark/ext_jars

# Imposta le variabili d'ambiente
ENV SPARK_EXTRA_CLASSPATH="/opt/bitnami/spark/ext_jars/*"
ENV PYTHONUNBUFFERED=1

USER 1001
WORKDIR /app
COPY src/delta/bronze/sensors_consumers/ .

# Installa librerie Python necessarie
RUN pip install --no-cache-dir pyspark==3.4.1 delta-spark==2.4.0 psycopg2-binary bcrypt boto3

# Comando finale
CMD spark-submit --master local[*] \
 --jars /opt/bitnami/spark/ext_jars/delta-core_2.12-2.4.0.jar,/opt/bitnami/spark/ext_jars/delta-storage-2.4.0.jar,/opt/bitnami/spark/ext_jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/bitnami/spark/ext_jars/kafka-clients-3.5.1.jar \
 bpm_consumer.py

