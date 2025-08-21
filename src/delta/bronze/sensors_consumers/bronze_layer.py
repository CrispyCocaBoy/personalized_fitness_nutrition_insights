from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, expr
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# ========================
# 1️⃣ Spark + MinIO
# ========================
try:
    spark = SparkSession.builder \
        .appName("BronzeLayerOptimized") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Configurazione MinIO (S3A)
    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "minioadmin")
    hadoopConf.set("fs.s3a.secret.key", "minioadmin")
    hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    print("✅ Spark session creata e configurazione MinIO caricata")

except Exception as e:
    print("❌ ERRORE nella creazione SparkSession o configurazione MinIO:", e)
    raise

# ========================
# 2️⃣ Schema e topic
# ========================
sensor_topics = [
    "wearables.ppg.raw",
    "wearables.skin-temp.raw",
    "wearables.accelerometer.raw",
    "wearables.gyroscope.raw",
    "wearables.altimeter.raw",
    "wearables.barometer.raw",
    "wearables.ceda.raw"
]

sensor_schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", LongType()) \
    .add("metric", StringType()) \
    .add("value", DoubleType())

# ========================
# 3️⃣ Funzione per creare stream con logging batch
# ========================
def create_stream(topic):
    try:
        df_kafka = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "broker_kafka:9092") \
            .option("subscribe", topic) \
            .option("minPartitions", "6") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        print(f"✅ readStream avviato per topic: {topic}")

        df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), sensor_schema).alias("data")) \
            .select("data.*") \
            .withColumn("ingest_ts", current_timestamp()) \
            .withColumn(
                "latency_sec",
                expr("(unix_timestamp() * 1000 - timestamp) / 1000.0").cast(DoubleType())
            )

        # Funzione foreachBatch con logging
        def write_and_log(batch_df, batch_id):
            if batch_df.isEmpty():
                print(f"⚠️ Batch {batch_id} vuoto per topic {topic}")
                return

            batch_df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(f"s3a://bronze/{topic}/")

            print(f"✅ Batch {batch_id} topic {topic}: scritti {batch_df.count()} record su Delta")

        query = (
            df_parsed.writeStream
            .foreachBatch(write_and_log)
            .outputMode("append")
            .trigger(processingTime="5 second")  # intervallo microbatch
            .option("checkpointLocation", f"s3a://bronze/checkpoints/{topic}/")
            .start()
        )

        print(f"✅ Streaming query avviata per topic: {topic}")
        return query

    except Exception as e:
        print(f"❌ ERRORE nello stream del topic {topic}:", e)
        raise

# ========================
# 4️⃣ Avvio streaming multipli
# ========================
queries = [create_stream(topic) for topic in sensor_topics]

try:
    for q in queries:
        q.awaitTermination()
except KeyboardInterrupt:
    print("🛑 Interruzione manuale delle query streaming.")
finally:
    print("⏹ Arresto job Bronze Layer richiesto dall'utente")
    for q in queries:
        q.stop()
    spark.stop()
