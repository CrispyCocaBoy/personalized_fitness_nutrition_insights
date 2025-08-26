# jobs/activities_enrichment_from_kafka.py
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date, lit, when, avg, sum as Fsum,
    max as Fmax, expr
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable

# ========================
# Config
# ========================
S3_ENDPOINT     = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY   = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY   = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_REGION       = os.getenv("S3_REGION", "us-east-1")
S3_BUCKET_GOLD  = os.getenv("S3_BUCKET_GOLD", "gold")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
ACTIVITIES_TOPIC = os.getenv("ACTIVITIES_TOPIC", "activities")

METRICS_FACT_PATH     = f"s3a://{S3_BUCKET_GOLD}/metrics_fact"
ACTIVITIES_FACT_PATH  = f"s3a://{S3_BUCKET_GOLD}/activities_fact"
ACTIVITIES_DAILY_PATH = f"s3a://{S3_BUCKET_GOLD}/activities_daily"
CHECKPOINT_DIR        = f"s3a://{S3_BUCKET_GOLD}/checkpoints/gold_activity"

# stima lunghezza passo: 70 cm
STEP_M = float(os.getenv("STEP_METERS", "0.7"))

# ========================
# Spark Session
# ========================
spark = (
    SparkSession.builder.appName("gold_activity")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # MinIO / S3A
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.region", S3_REGION)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(S3_ENDPOINT.lower().startswith("https")).lower())
    # Delta consigliate
    .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ========================
# Schemi
# ========================
# NB: includo 'task' per distinguere add/delete
activity_schema = StructType([
    StructField("task",              StringType(),  True),   # "add" | "delete" | None (= add)
    StructField("activity_event_id", StringType(),  False),
    StructField("user_id",           StringType(),  False),
    StructField("activity_id",       IntegerType(), True),
    StructField("activity_name",     StringType(),  True),
    StructField("start_ts",          StringType(),  True),   # ISO (atteso con Z)
    StructField("end_ts",            StringType(),  True),   # ISO (atteso con Z)
    StructField("notes",             StringType(),  True)
])

# ========================
# Lettura Kafka -> JSON
# ========================
raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("subscribe", ACTIVITIES_TOPIC)
         .option("startingOffsets", "latest")
         .option("failOnDataLoss", "false")
         .load()
)

events = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), activity_schema).alias("e"))
       .select("e.*")
)

# ========================
# foreachBatch: gestisce add & delete
# ========================
def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    # --- Split ADD / DELETE all'interno del batch (batch_df è STATICO) ---
    adds = (
        batch_df
        .filter((col("task").isNull()) | (col("task") == lit("add")))
        .filter(col("activity_event_id").isNotNull() & col("user_id").isNotNull())
        .withColumn("start_ts", to_timestamp(col("start_ts")))
        .withColumn("end_ts",   to_timestamp(col("end_ts")))
        .withColumn("event_date", to_date(col("start_ts")))
    )

    deletes = (
        batch_df
        .filter(col("task") == lit("delete"))
        .filter(col("activity_event_id").isNotNull() & col("user_id").isNotNull())
        .select("activity_event_id", "user_id")
        .dropDuplicates(["activity_event_id", "user_id"])
    )

    fact_exists = DeltaTable.isDeltaTable(spark, ACTIVITIES_FACT_PATH)
    fact_tbl = DeltaTable.forPath(spark, ACTIVITIES_FACT_PATH) if fact_exists else None
    fact_df  = fact_tbl.toDF() if fact_exists else None

    # ========= DELETE =========
    touched_del = None
    if not deletes.rdd.isEmpty() and fact_exists:
        # Ricavo (user_id, event_date) delle righe da cancellare
        del_target = (
            deletes.alias("k")
                   .join(
                       fact_df.alias("f"),
                       (col("k.activity_event_id") == col("f.activity_event_id")) &
                       (col("k.user_id") == col("f.user_id")),
                       "inner"
                   )
                   .select(col("f.activity_event_id"), col("f.user_id"), col("f.event_date"))
                   .dropDuplicates(["activity_event_id", "user_id"])
        )
        ids = [r["activity_event_id"] for r in del_target.select("activity_event_id").distinct().collect()]
        if ids:
            fact_tbl.delete(col("activity_event_id").isin(ids))
        touched_del = del_target.select("user_id", "event_date").dropDuplicates()

    # ========= ADD =========
    touched_add = None
    if not adds.rdd.isEmpty():
        # Carico metriche utili
        metrics = (
            spark.read.format("delta").load(METRICS_FACT_PATH)
                 .select("user_id", "window_start", "window_end", "hr_bpm", "calories", "step_count")
                 .withColumn("window_start", to_timestamp(col("window_start")))
                 .withColumn("window_end",   to_timestamp(col("window_end")))
        )

        # Join per sovrapposizione intervalli: (m.end > act.start) AND (m.start < act.end)
        joined = (
            adds.alias("act")
                .join(
                    metrics.alias("m"),
                    (col("act.user_id") == col("m.user_id")) &
                    (col("m.window_end")   > col("act.start_ts")) &
                    (col("m.window_start") < col("act.end_ts")),
                    "left"
                )
        )

        # KPI per attività
        agg = (
            joined.groupBy(
                "act.activity_event_id", "act.user_id", "act.activity_id", "act.activity_name",
                "act.start_ts", "act.end_ts", "act.event_date", "act.notes"
            )
            .agg(
                avg("m.hr_bpm").alias("hr_bpm_avg"),
                Fsum("m.calories").alias("calories_total"),
                Fsum("m.step_count").alias("steps_total"),
            )
            .withColumn("steps_total", when(col("steps_total").isNull(), lit(0)).otherwise(col("steps_total")))
            .withColumn("calories_total", when(col("calories_total").isNull(), lit(0)).otherwise(col("calories_total")))
            .withColumn("duration_min", (col("end_ts").cast("long") - col("start_ts").cast("long")) / 60.0)
            .withColumn("distance_m", col("steps_total") * lit(STEP_M))
            .withColumn("pace_m_per_min", when(col("duration_min") > 0, col("distance_m") / col("duration_min")))
            .withColumn("gold_ingest_ts", lit(datetime.utcnow().isoformat() + "Z"))
            .select(
                "activity_event_id","user_id","activity_id","activity_name",
                "start_ts","end_ts","event_date","notes",
                "duration_min","hr_bpm_avg","calories_total","steps_total",
                "distance_m","pace_m_per_min","gold_ingest_ts"
            )
        )

        # Upsert nel FACT
        if DeltaTable.isDeltaTable(spark, ACTIVITIES_FACT_PATH):
            tgt = DeltaTable.forPath(spark, ACTIVITIES_FACT_PATH)
            (tgt.alias("t")
                .merge(agg.alias("s"), "t.activity_event_id = s.activity_event_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
        else:
            agg.write.format("delta").mode("overwrite").save(ACTIVITIES_FACT_PATH)

        touched_add = agg.select("user_id", "event_date").dropDuplicates()

    # ========= Recompute DAILY per partizioni toccate =========
    if DeltaTable.isDeltaTable(spark, ACTIVITIES_FACT_PATH):
        fact_now = spark.read.format("delta").load(ACTIVITIES_FACT_PATH)

        if touched_add is not None and touched_del is not None:
            touched = touched_add.unionByName(touched_del).dropDuplicates()
        elif touched_add is not None:
            touched = touched_add
        else:
            touched = touched_del

        if touched is not None and not touched.rdd.isEmpty():
            daily = (
                fact_now.join(touched, on=["user_id", "event_date"], how="inner")
                        .groupBy("user_id", "event_date")
                        .agg(
                            Fsum("duration_min").alias("duration_total_min"),
                            Fmax("end_ts").alias("last_activity_end_ts"),
                            expr("COUNT(1)").alias("activities_count"),
                        )
            )

            if DeltaTable.isDeltaTable(spark, ACTIVITIES_DAILY_PATH):
                # overwrite selettivo delle partizioni toccate
                parts = [f"(user_id = '{r['user_id']}' AND event_date = DATE '{r['event_date']}')" for r in touched.collect()]
                if parts:
                    predicate = " OR ".join(parts)
                    (daily.write.format("delta")
                          .mode("overwrite")
                          .option("replaceWhere", predicate)
                          .save(ACTIVITIES_DAILY_PATH))
            else:
                daily.write.format("delta").mode("overwrite").save(ACTIVITIES_DAILY_PATH)


# ========================
# Avvio streaming
# ========================
(
    events.writeStream
          .outputMode("update")                 # non usiamo il sink, tutto in foreachBatch
          .option("checkpointLocation", CHECKPOINT_DIR)
          .foreachBatch(process_batch)
          .start()
          .awaitTermination()
)
