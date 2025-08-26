FROM bitnami/spark:3.5.5

# Usa root per installazioni
USER root
WORKDIR /app

# Installa le librerie Python
RUN pip install --no-cache-dir pyspark==3.5.5 delta-spark==3.3.1 redis boto3 pyarrow

# Torna all'utente non-root
USER 1001

# Copia gli script nello container
COPY src/delta/gold/meal_gold/ .

# Avvio dello script con i pacchetti necessari: Delta + Kafka
CMD spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name gold_meal \
    --packages io.delta:delta-spark_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.cores.max=1 \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=600m \
    --conf spark.driver.memory=500m \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    gold_meal.py

