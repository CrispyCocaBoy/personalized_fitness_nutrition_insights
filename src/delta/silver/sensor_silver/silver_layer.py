
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, expr, when, to_json, struct
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField
import redis
import time

# ============================
# Spark Session
# ============================
spark = (
    SparkSession.builder
    .appName("silver_layer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session created")

# ============================
# MinIO settings
# ============================
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
print("✅ Minio configured")

# ============================
# Kafka Schema
# ============================
sensor_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("metric", StringType()),
    StructField("value", DoubleType())
])

sensor_topics = [
    "wearables.ppg.raw",
    "wearables.skin-temp.raw",
    "wearables.accelerometer.raw",
    "wearables.gyroscope.raw",
    "wearables.altimeter.raw",
    "wearables.barometer.raw",
    "wearables.ceda.raw"
]

# ============================
# Read from Kafka
# ============================
df_parsed = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker_kafka:9092")
    .option("subscribe", ",".join(sensor_topics))
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as json", "topic")
    .select(from_json(col("json"), sensor_schema).alias("data"), col("topic"))
    .select("data.*", "topic")
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("latency_sec", expr("(unix_timestamp() * 1000 - timestamp) / 1000.0").cast(DoubleType()))
    .withColumn(
        "measurement_type",
        when(col("topic") == "wearables.ppg.raw", "ppg")
        .when(col("topic") == "wearables.skin-temp.raw", "skin_temp")
        .when(col("topic") == "wearables.accelerometer.raw", "accelerometer")
        .when(col("topic") == "wearables.gyroscope.raw", "gyroscope")
        .when(col("topic") == "wearables.altimeter.raw", "altimeter")
        .when(col("topic") == "wearables.barometer.raw", "barometer")
        .when(col("topic") == "wearables.ceda.raw", "ceda")
        .otherwise("unknown")
    )
)

# ============================
# CockroachDB (fallback) ENV
# ============================
import os
CRDB_HOST     = os.getenv("CRDB_HOST", "cockroachdb")
CRDB_PORT     = int(os.getenv("CRDB_PORT", "26257"))
CRDB_DBNAME   = os.getenv("CRDB_DBNAME", "user_device_db")
CRDB_USER     = os.getenv("CRDB_USER", "root")
CRDB_PASSWORD = os.getenv("CRDB_PASSWORD", "")
CRDB_SSLMODE  = os.getenv("CRDB_SSLMODE", "disable")
CRDB_TIMEOUT  = int(os.getenv("CRDB_TIMEOUT", "5"))


# ============================
# Redis Helper
# ============================
def load_sensor_mapping_from_db():
    """
    Fallback: legge sensor_to_user (sensor_id, user_id).
    Se esistono più righe per lo stesso sensor_id, prende la più recente per created_at.
    """
    try:
        dsn = (
            f"host={CRDB_HOST} port={CRDB_PORT} dbname={CRDB_DBNAME} "
            f"user={CRDB_USER} sslmode={CRDB_SSLMODE} connect_timeout={CRDB_TIMEOUT}"
        )
        if CRDB_PASSWORD:
            dsn += f" password={CRDB_PASSWORD}"

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                # Cockroach compatibile: usa window function per prendere la riga più recente per sensor_id
                cur.execute("""
                    SELECT sensor_id, user_id
                    FROM (
                        SELECT sensor_id, user_id,
                               row_number() OVER (PARTITION BY sensor_id ORDER BY created_at DESC) AS rn
                        FROM sensor_to_user
                    ) t
                    WHERE rn = 1
                """)
                rows = cur.fetchall()
                mapping = {str(sensor_id): str(user_id) for (sensor_id, user_id) in rows}
                print(f"ℹ️ Fallback DB mapping loaded: {len(mapping)} entries")
                return mapping
    except Exception as e:
        print(f"⚠️ CockroachDB fallback failed: {e}")
        return {}

def load_sensor_mapping_from_redis():
    """
    Scarica tutta la mappa sensor_id→user_id da Redis (DB=1).
    Se vuota o errore, usa fallback su CockroachDB (sensor_to_user).
    """
    try:
        r = redis.StrictRedis(host="redis", port=6379, db=1, decode_responses=True)
        keys = r.keys("*")
        mapping = {}
        if keys:
            pipe = r.pipeline()
            for k in keys:
                pipe.get(k)
            values = pipe.execute()
            mapping = dict(zip(keys, values))
        if mapping:
            print(f"✅ Redis mapping loaded: {len(mapping)} entries")
            return mapping
        # fallback se vuoto
        print("ℹ️ Redis empty → trying CockroachDB fallback…")
        return load_sensor_mapping_from_db()
    except Exception as e:
        print(f"⚠️ Redis not reachable: {e} → trying CockroachDB fallback…")
        return load_sensor_mapping_from_db()

# broadcast iniziale
sensor_mapping = load_sensor_mapping_from_redis()
broadcast_mapping = spark.sparkContext.broadcast(sensor_mapping)
last_refresh = time.time()

def refresh_mapping():
    """Aggiorna broadcast mapping ogni 5 minuti"""
    global broadcast_mapping, last_refresh
    now = time.time()
    if now - last_refresh > 300:
        new_map = load_sensor_mapping_from_redis()
        if new_map:
            broadcast_mapping.unpersist()
            broadcast_mapping = spark.sparkContext.broadcast(new_map)
            last_refresh = now

# ============================
# ForeachBatch function
# ============================
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    refresh_mapping()
    mapping = broadcast_mapping.value

    def add_user_id(rows):
        for row in rows:
            uid = mapping.get(str(row.sensor_id))
            if uid is not None:
                yield (*row, uid)

    schema_with_user = StructType(batch_df.schema.fields + [StructField("user_id", StringType(), True)])

    enriched_df = spark.createDataFrame(
        batch_df.rdd.mapPartitions(add_user_id), schema=schema_with_user
    )

    if enriched_df.isEmpty():
        print(f"⚠️ Batch {batch_id}: nessun sensore mappato")
        return

    enriched_df.cache()

    # 1️⃣ Scrittura su Delta
    (
        enriched_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("user_id")
        .save("s3a://silver/sensor_data/")
    )

    # 2️⃣ Scrittura su Kafka
    kafka_df = enriched_df.selectExpr(
        "CAST(user_id AS STRING) as key",
        "to_json(struct(*)) as value"
    )

    (
        kafka_df
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker_kafka:9092")
        .option("topic", "silver_layer")
        .save()
    )

    print(f"✅ Batch {batch_id}: scritti {enriched_df.count()} record → Delta + Kafka")

    enriched_df.unpersist()

# ============================
# Start Streaming Query
# ============================
query = (
    df_parsed.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "s3a://silver/checkpoints/silver_per_user_dual/")
    .start()
)

print("✅ Streaming Silver Layer (Delta + Kafka) avviato")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
finally:
    spark.stop()
