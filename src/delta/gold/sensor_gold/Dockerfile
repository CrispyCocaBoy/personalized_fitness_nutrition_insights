FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa le librerie Python
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1 redis boto3 pyarrow numpy psycopg[binary]


# Torna all'utente non-root
USER 1001

# Copia gli script nello container
COPY src/delta/gold/sensor_gold/ .

# Avvio dello script con i pacchetti necessari: Delta + Kafka
CMD spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  gold_layer.py

