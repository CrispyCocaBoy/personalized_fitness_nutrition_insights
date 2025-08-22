# jobs/gold_meals_from_kafka_optimized.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, regexp_replace, current_timestamp,
    to_date, lit, sum as Fsum, count as Fcount, max as Fmax, expr
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable

# ========================
# Config from env
# ========================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
MEALS_TOPIC     = os.getenv("MEALS_TOPIC", "meals")
START_OFFSETS   = os.getenv("STARTING_OFFSETS", "latest")   # "latest" | "earliest" | JSON
FAIL_ON_LOSS    = os.getenv("FAIL_ON_DATA_LOSS", "false")
TRIGGER         = os.getenv("TRIGGER", "30 seconds")
MAX_OFFSETS     = os.getenv("MAX_OFFSETS_PER_TRIGGER", "")  # es. "50000"

S3_ENDPOINT     = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY   = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY   = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_GOLD  = os.getenv("S3_BUCKET_GOLD", "gold")

FACT_PATH       = f"s3a://{S3_BUCKET_GOLD}/meals_fact"   # partiz. (user_id,event_date)
DAILY_PATH      = f"s3a://{S3_BUCKET_GOLD}/meals_daily"  # merge su (user_id,event_date)
CKP_STREAM      = f"s3a://{S3_BUCKET_GOLD}/checkpoints/gold_meals"

# ========================
# Spark & Delta
# ========================
spark = (
    SparkSession.builder
    .appName("GoldMeals (Fact + Daily) — Optimized")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Delta compattazioni automatiche (se supportate)
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# MinIO / S3A
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", S3_ACCESS_KEY)
hadoopConf.set("fs.s3a.secret.key", S3_SECRET_KEY)
hadoopConf.set("fs.s3a.endpoint", S3_ENDPOINT)
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoopConf.set("fs.s3a.fast.upload", "true")
hadoopConf.set("fs.s3a.connection.maximum", "200")
hadoopConf.set("fs.s3a.threads.max", "200")
hadoopConf.set("fs.s3a.multipart.size", "134217728")  # 128MB
hadoopConf.set("fs.s3a.fast.upload.buffer", "disk")

print("✅ Spark ready & MinIO configured (UTC)")

# ========================
# Schema JSON atteso dal gateway
# ========================
meal_schema = StructType([
    StructField("meal_id",   StringType(),  True),
    StructField("user_id",   StringType(),  True),
    StructField("meal_name", StringType(),  True),
    StructField("kcal",      IntegerType(), True),
    StructField("carbs_g",   IntegerType(), True),
    StructField("protein_g", IntegerType(), True),
    StructField("fat_g",     IntegerType(), True),
    StructField("timestamp", StringType(),  True),   # es. "2025-08-20T10:15:00Z"
    StructField("notes",     StringType(),  True),
    StructField("ingest_ts", StringType(),  True),
    StructField("source",    StringType(),  True),
    StructField("schema_version", StringType(), True),
])

# ========================
# Lettura Kafka → parsing JSON
# ========================
kafka_reader = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", MEALS_TOPIC)
    .option("startingOffsets", START_OFFSETS)
    .option("failOnDataLoss", FAIL_ON_LOSS)
)
if MAX_OFFSETS.strip():
    kafka_reader = kafka_reader.option("maxOffsetsPerTrigger", MAX_OFFSETS.strip())

raw = kafka_reader.load()

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), meal_schema).alias("m"))
       .select("m.*")
)

# Timestamp ISO8601 con suffisso Z→UTC
fact_meals_stream = (
    parsed
    .withColumn(
        "event_ts",
        # supporta "2025-08-20T10:15:00Z" e con millisecondi
        to_timestamp(regexp_replace(col("timestamp"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss[.SSS]")
    )
    .withColumn("event_date", to_date(col("event_ts")))
    .withColumn("gold_ingest_ts", current_timestamp())
    .select(
        "meal_id","user_id","meal_name","kcal","carbs_g","protein_g","fat_g",
        "notes","event_ts","event_date","ingest_ts","source","schema_version","gold_ingest_ts"
    )
)

# ========================
# ForeachBatch: write FACT + MERGE DAILY (una volta per batch)
# ========================
def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        print(f"⚠️ Batch {batch_id} vuoto")
        return

    # FACT (scrittura unica partizionata)
    (batch_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("user_id", "event_date")
        .save(FACT_PATH))

    # DAILY (aggregazione e 1 MERGE)
    daily = (
        batch_df.groupBy("user_id", "event_date")
                .agg(
                    Fsum("kcal").alias("kcal_total"),
                    Fsum("carbs_g").alias("carbs_total_g"),
                    Fsum("protein_g").alias("protein_total_g"),
                    Fsum("fat_g").alias("fat_total_g"),
                    Fcount(lit(1)).alias("meals_count"),
                    Fmax("event_ts").alias("last_meal_ts"),
                )
                .withColumn("gold_ingest_ts", current_timestamp())
    )

    if DeltaTable.isDeltaTable(spark, DAILY_PATH):
        tgt = DeltaTable.forPath(spark, DAILY_PATH)
        (tgt.alias("t")
            .merge(
                daily.alias("s"),
                "t.user_id = s.user_id AND t.event_date = s.event_date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        (daily.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .save(DAILY_PATH))

    print(f"✅ Batch {batch_id}: FACT → {FACT_PATH} | DAILY MERGE → {DAILY_PATH}")

# ========================
# Avvio streaming
# ========================
query = (
    fact_meals_stream.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")                 # richiesto dall’API
    .trigger(processingTime=TRIGGER)
    .option("checkpointLocation", CKP_STREAM)
    .start()
)

print(f"🚀 GoldMeals avviato — FACT: {FACT_PATH} | DAILY: {DAILY_PATH}")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("⏹ Stop richiesto")
    query.stop()
    spark.stop()
