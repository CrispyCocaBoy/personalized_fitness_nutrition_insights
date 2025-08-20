# jobs/gold_meals_from_kafka.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, regexp_replace, current_timestamp,
    to_date, lit, sum as Fsum, count as Fcount, max as Fmax
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from delta.tables import DeltaTable

# ========================
# Config from env
# ========================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
MEALS_TOPIC     = os.getenv("MEALS_TOPIC", "meals")
START_OFFSETS   = os.getenv("STARTING_OFFSETS", "latest")   # "latest" | "earliest" | JSON
FAIL_ON_LOSS    = os.getenv("FAIL_ON_DATA_LOSS", "false")

S3_ENDPOINT     = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY   = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY   = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_GOLD  = os.getenv("S3_BUCKET_GOLD", "gold")

# Base path per utente
USERS_BASE = f"s3a://{S3_BUCKET_GOLD}/users"

# Checkpoint streaming
CKP_STREAM  = f"s3a://{S3_BUCKET_GOLD}/checkpoints/gold_meals_per_user"

# ========================
# Spark & Delta
# ========================
spark = (
    SparkSession.builder
    .appName("GoldLayerMealsPerUserFromKafka")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Config MinIO (S3A)
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", S3_ACCESS_KEY)
hadoopConf.set("fs.s3a.secret.key", S3_SECRET_KEY)
hadoopConf.set("fs.s3a.endpoint", S3_ENDPOINT)
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print("✅ Spark ready & MinIO configured")

# ========================
# Schema JSON atteso dal gateway
# ========================
meal_schema = StructType([
    StructField("meal_id",   StringType(),  True),
    StructField("user_id",   StringType(),  True),
    StructField("meal_name", StringType(),  True),
    StructField("kcal",      IntegerType(), True),       # <-- coerente
    StructField("carbs_g",   IntegerType(), True),       # <-- coerente
    StructField("protein_g", IntegerType(), True),       # <-- coerente
    StructField("fat_g",     IntegerType(), True),       # <-- coerente
    StructField("timestamp", StringType(),  True),       # es. "2025-08-20T10:15:00Z"
    StructField("notes",     StringType(),  True),
    StructField("ingest_ts", StringType(),  True),       # opzionale dal gateway
    StructField("source",    StringType(),  True),       # opzionale
    StructField("schema_version", StringType(), True),   # opzionale
])

# ========================
# Lettura Kafka → parsing JSON
# ========================
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", MEALS_TOPIC)
    .option("startingOffsets", START_OFFSETS)
    .option("failOnDataLoss", FAIL_ON_LOSS)
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), meal_schema).alias("m"))
       .select("m.*")
)

# Normalizzazione timestamp ISO8601 con suffisso Z (UTC) → event_ts/event_date
fact_meals_stream = (
    parsed
    .withColumn("event_ts",
        to_timestamp(regexp_replace(col("timestamp"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss[.SSS]")
    )
    .withColumn("event_date", to_date(col("event_ts")))
    .withColumn("gold_ingest_ts", current_timestamp())
    .select(
        col("meal_id"),
        col("user_id"),
        col("meal_name"),
        col("kcal"),
        col("carbs_g"),
        col("protein_g"),
        col("fat_g"),
        col("notes"),
        col("event_ts"),
        col("event_date"),
        col("ingest_ts"),
        col("source"),
        col("schema_version"),
        col("gold_ingest_ts")
    )
)

# ========================
# ForeachBatch: scrittura PER-UTENTE
# ========================
def write_per_user(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        print(f"⚠️ Batch {batch_id} vuoto")
        return

    # user_id presenti nel batch
    users = [r.user_id for r in batch_df.select("user_id").distinct().collect()]
    print(f"ℹ️ Batch {batch_id}: utenti nel batch = {users}")

    for uid in users:
        u_df = batch_df.filter(col("user_id") == uid)

        # --- FACT per utente ---
        fact_path = f"{USERS_BASE}/{uid}/meals"
        (
            u_df.coalesce(1)  # evita troppi file piccoli per utente
              .write.format("delta")
              .mode("append")
              .option("mergeSchema", "true")
              .partitionBy("event_date")
              .save(fact_path)
        )

        # --- AGG giornaliero per utente ---
        daily = (
            u_df.groupBy("user_id", "event_date")
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

        daily_path = f"{USERS_BASE}/{uid}/meals_daily"
        if DeltaTable.isDeltaTable(spark, daily_path):
            tgt = DeltaTable.forPath(spark, daily_path)
            (
                tgt.alias("t")
                   .merge(
                       daily.alias("s"),
                       "t.user_id = s.user_id AND t.event_date = s.event_date"
                    )
                   .whenMatchedUpdateAll()
                   .whenNotMatchedInsertAll()
                   .execute()
            )
        else:
            (
                daily.write
                     .format("delta")
                     .mode("overwrite")
                     .option("overwriteSchema", "true")
                     .save(daily_path)
            )

        print(f"✅ Batch {batch_id}: scritto user_id={uid} → {fact_path} + {daily_path}")

# ========================
# Avvio streaming
# ========================
query = (
    fact_meals_stream.writeStream
    .foreachBatch(write_per_user)
    .outputMode("update")                # richiesto dall'API, non usato direttamente
    .option("checkpointLocation", CKP_STREAM)
    .start()
)

print(f"🚀 Gold per-utente avviato — base path: {USERS_BASE}/<user_id>/")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("⏹ Stop richiesto")
    query.stop()
    spark.stop()
