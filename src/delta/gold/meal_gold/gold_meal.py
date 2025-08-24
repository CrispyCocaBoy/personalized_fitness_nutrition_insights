# jobs/gold_meals_from_kafka_fast.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, regexp_replace, current_timestamp,
    to_date, lit, sum as Fsum, count as Fcount, max as Fmax, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable

# ========================
# Config from env
# ========================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
MEALS_TOPIC     = os.getenv("MEALS_TOPIC", "meals")
START_OFFSETS   = os.getenv("STARTING_OFFSETS", "latest")
FAIL_ON_LOSS    = os.getenv("FAIL_ON_DATA_LOSS", "false")
TRIGGER         = os.getenv("TRIGGER", "5 seconds")
MAX_OFFSETS     = os.getenv("MAX_OFFSETS_PER_TRIGGER", "")

S3_ENDPOINT     = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY   = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY   = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_GOLD  = os.getenv("S3_BUCKET_GOLD", "gold")

FACT_PATH       = f"s3a://{S3_BUCKET_GOLD}/meals_fact"
DAILY_PATH      = f"s3a://{S3_BUCKET_GOLD}/meals_daily"
CKP_STREAM      = f"s3a://{S3_BUCKET_GOLD}/checkpoints/gold_meals"

FACT_WRITE_PARTITIONS = int(os.getenv("FACT_WRITE_PARTITIONS", "4"))

# ========================
# Spark & Delta
# ========================
spark = (
    SparkSession.builder
    .appName("GoldMeals Fast")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# MinIO
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", S3_ACCESS_KEY)
hadoopConf.set("fs.s3a.secret.key", S3_SECRET_KEY)
hadoopConf.set("fs.s3a.endpoint", S3_ENDPOINT)
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoopConf.set("fs.s3a.fast.upload", "true")
hadoopConf.set("fs.s3a.connection.maximum", "200")
hadoopConf.set("fs.s3a.threads.max", "200")
hadoopConf.set("fs.s3a.multipart.size", "134217728")
hadoopConf.set("fs.s3a.fast.upload.buffer", "disk")

print("✅ Spark ready & MinIO configured")

# ========================
# Schema
# ========================
meal_schema = StructType([
    StructField("task",   StringType(),  True),
    StructField("meal_id",   StringType(),  True),
    StructField("user_id",   StringType(),  True),
    StructField("meal_name", StringType(),  True),
    StructField("kcal",      IntegerType(), True),
    StructField("carbs_g",   IntegerType(), True),
    StructField("protein_g", IntegerType(), True),
    StructField("fat_g",     IntegerType(), True),
    StructField("timestamp", StringType(),  True),
    StructField("notes",     StringType(),  True),
    StructField("ingest_ts", StringType(),  True),
    StructField("source",    StringType(),  True),
    StructField("schema_version", StringType(), True),
])

# ========================
# Kafka reader
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

with_event = (
    parsed
    .withColumn(
        "event_ts",
        to_timestamp(regexp_replace(col("timestamp"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss[.SSS]")
    )
    .withColumn("event_date", to_date(col("event_ts")))
    .withColumn("gold_ingest_ts", current_timestamp())
)

# ========================
# ForeachBatch
# ========================
def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    batch_df = batch_df.cache()

    adds_df = (
        batch_df.filter((col("task").isNull()) | (col("task") == lit("add")))
    )
    deletes_df = (
        batch_df.filter(col("task") == lit("delete"))
                .select("meal_id", "user_id", "event_date")
                .distinct()
    )

    # --- ADD ---
    if not adds_df.rdd.isEmpty():
        adds_df_opt = adds_df.repartition(
            FACT_WRITE_PARTITIONS, col("user_id"), col("event_date")
        )
        (adds_df_opt.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("user_id", "event_date")
            .save(FACT_PATH))

        daily_adds = (
            adds_df_opt.groupBy("user_id", "event_date")
                   .agg(
                       Fsum("kcal").alias("kcal_total"),
                       Fsum("carbs_g").alias("carbs_total_g"),
                       Fsum("protein_g").alias("protein_total_g"),
                       Fsum("fat_g").alias("fat_total_g"),
                       Fcount(lit(1)).alias("meals_count"),
                       Fmax("event_ts").alias("last_meal_ts"),
                   )
                   .withColumn("gold_ingest_ts", current_timestamp())
                   .coalesce(1)
        )
        if DeltaTable.isDeltaTable(spark, DAILY_PATH):
            tgt = DeltaTable.forPath(spark, DAILY_PATH)
            (tgt.alias("t")
               .merge(
                   daily_adds.alias("s"),
                   "t.user_id = s.user_id AND t.event_date = s.event_date"
               )
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute())
        else:
            (daily_adds.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(DAILY_PATH))

    # --- DELETE ---
    if not deletes_df.rdd.isEmpty():
        fact_dt = DeltaTable.forPath(spark, FACT_PATH)
        (fact_dt.alias("t")
            .merge(
                deletes_df.alias("d"),
                "t.user_id = d.user_id AND t.meal_id = d.meal_id"
            )
            .whenMatchedDelete()
            .execute())

        affected_keys = deletes_df.select("user_id", "event_date").distinct()
        fact_now = spark.read.format("delta").load(FACT_PATH)

        recalculated = (
            fact_now.join(broadcast(affected_keys), ["user_id", "event_date"], "inner")
                    .groupBy("user_id", "event_date")
                    .agg(
                        Fsum("kcal").alias("kcal_total"),
                        Fsum("carbs_g").alias("carbs_total_g"),
                        Fsum("protein_g").alias("protein_total_g"),
                        Fsum("fat_g").alias("fat_total_g"),
                        Fcount(lit(1)).alias("meals_count"),
                        Fmax("event_ts").alias("last_meal_ts"),
                    )
                    .withColumn("gold_ingest_ts", current_timestamp())
                    .coalesce(1)
        )
        if not recalculated.rdd.isEmpty():
            tgt = DeltaTable.forPath(spark, DAILY_PATH) if DeltaTable.isDeltaTable(spark, DAILY_PATH) else None
            if tgt is None:
                (recalculated.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .save(DAILY_PATH))
            else:
                (tgt.alias("t")
                    .merge(
                        recalculated.alias("s"),
                        "t.user_id = s.user_id AND t.event_date = s.event_date"
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute())

    batch_df.unpersist(False)
    print(f"✅ Batch {batch_id} — adds={adds_df.count()} deletes={deletes_df.count()}")

# ========================
# Avvio streaming
# ========================
query = (
    with_event.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .trigger(processingTime=TRIGGER)
    .option("checkpointLocation", CKP_STREAM)
    .start()
)

print(f"🚀 GoldMeals FAST — FACT={FACT_PATH} DAILY={DAILY_PATH}")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
