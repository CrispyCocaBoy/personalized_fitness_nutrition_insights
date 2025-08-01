from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, expr, when, to_json, struct
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField
import redis

# SparkSession
spark = (
    SparkSession.builder
    .appName("SilverLayerPerUserWithRedisLookup")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session created")

# MinIO setting
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print("✅ Minio configured")

# =========================================
# Schema Kafka
# =========================================
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

# Lettura streaming da Kafka
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

# Redis Connection Helper
def get_redis_connection():
    try:
        r = redis.StrictRedis(
            host='redis', port=6379, db=1,
            decode_responses=True,
            socket_timeout=2, socket_connect_timeout=2,
            retry_on_timeout=True
        )
        r.ping()
        return r
    except:
        return None

# =========================================
# foreachBatch con lookup Redis + Kafka
# =========================================
def enrich_with_redis(batch_df, batch_id):
    if not batch_df.take(1):
        print(f"⚠️ Batch {batch_id} vuoto")
        return

    def redis_lookup_partition(rows):
        client = get_redis_connection()
        rows_list = list(rows)
        if not client:
            return [(*row, None) for row in rows_list]

        keys = [f"sensor:{row.sensor_id}" for row in rows_list]
        pipe = client.pipeline()
        for k in keys:
            pipe.get(k)
        user_ids = pipe.execute()

        try:
            client.close()
        except:
            pass

        return [(*row, user_id) for row, user_id in zip(rows_list, user_ids)]

    schema_with_user = StructType(batch_df.schema.fields + [StructField("user_id", StringType(), True)])
    enriched_df = spark.createDataFrame(batch_df.rdd.mapPartitions(redis_lookup_partition), schema=schema_with_user)

    valid_data = enriched_df.filter(col("user_id").isNotNull())
    if not valid_data.take(1):
        print(f"⚠️ Batch {batch_id}: nessun sensore mappato in Redis")
        return

    # 1️⃣ Scrittura Delta
    (
        valid_data
        .coalesce(1)
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("user_id")
        .save("s3a://silver/sensor_data/")
    )

    # 2️⃣ Scrittura Kafka topic silver_layer
    kafka_df = valid_data.selectExpr(
        "CAST(user_id AS STRING) as key",
        "to_json(struct(*)) as value"
    )

    (
        kafka_df
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker_kafka:9092")
        .option("topic", "silver_layer")
        .mode("append")
        .save()
    )

    print(f"✅ Batch {batch_id}: scritti {valid_data.count()} record su Delta + Kafka")

# =========================================
# Streaming Query
# =========================================
query = (
    df_parsed.writeStream
    .foreachBatch(enrich_with_redis)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "s3a://silver/checkpoints/silver_per_user_redis_lookup/")
    .start()
)

print("✅ Streaming Silver Layer con lookup Redis + Kafka avviato")
print("📁 Output in s3a://silver/sensor_data/ e Kafka topic silver_layer")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("⏹Arresto job richiesto dall'utente")
    query.stop()
finally:
    spark.stop()
