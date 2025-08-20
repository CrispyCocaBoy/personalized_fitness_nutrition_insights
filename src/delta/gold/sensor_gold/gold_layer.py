from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, expr, current_timestamp, when, avg, stddev, count,
    collect_list, window, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, FloatType
)
from pyspark.sql.functions import udf

# ========================
# Spark & MinIO
# ========================
spark = (
    SparkSession.builder
    .appName("GoldLayerMinimalPerUser")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
print("✅ Spark ready & MinIO configured")

# ========================
# Schema Silver
# ========================
silver_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("metric", StringType()),
    StructField("value", DoubleType()),
    StructField("topic", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("latency_sec", DoubleType()),
    StructField("measurement_type", StringType()),
    StructField("user_id", StringType())
])

df_silver = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker_kafka:9092")
    .option("subscribe", "silver_layer")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), silver_schema).alias("data"))
    .select("data.*")
    .withColumn("datetime", expr("from_unixtime(timestamp/1000)").cast("timestamp"))
)

# ========================
# UDF HR da PPG
# ========================
def _hr_from_ppg(values, timestamps_ms):
    try:
        import numpy as np
        if not values or not timestamps_ms or len(values) < 20:
            return None
        v = np.asarray(values, dtype=float)
        t = np.asarray(timestamps_ms, dtype=float)
        v = v - np.median(v)
        mad = np.median(np.abs(v)) + 1e-8
        v = v / (mad * 6.0)
        dt = np.diff(t)
        dt = dt[dt > 0]
        if dt.size == 0:
            return None
        fs = 1000.0 / float(np.median(dt))
        thr = float(np.mean(v) + 0.4*np.std(v))
        min_rr_n = max(1, int(0.30 * fs))
        peaks, last = [], -10**9
        for i in range(1, len(v)-1):
            if v[i] > thr and v[i] > v[i-1] and v[i] >= v[i+1]:
                if i - last >= min_rr_n:
                    peaks.append(i); last = i
        if len(peaks) < 2:
            return None
        rr = []
        for k in range(1, len(peaks)):
            dtms = t[peaks[k]] - t[peaks[k-1]]
            if 300.0 < dtms < 2000.0:
                rr.append(dtms)
        if len(rr) < 2:
            return None
        hr = 60000.0 / float(np.mean(rr))
        return float(np.clip(hr, 35.0, 210.0))
    except Exception:
        return None

hr_from_ppg_udf = udf(_hr_from_ppg, FloatType())

# ========================
# ForeachBatch
# ========================
def process_batch(batch_df, batch_id):
    if not batch_df.take(1):
        print(f"⚠️ Batch {batch_id} vuoto")
        return

    # --- PPG: HR + SPO2 proxy + Stress ---
    ppg_agg = (
        batch_df
        .filter(col("measurement_type") == "ppg")
        .withWatermark("datetime", "2 minutes")
        .groupBy(col("user_id"), window(col("datetime"), "1 minute"))
        .agg(
            collect_list("value").alias("ppg_values"),
            collect_list("timestamp").alias("ppg_ts"),
            avg(when(col("metric") == "ppg_ir", col("value"))).alias("ir_avg"),
            avg(when(col("metric") == "ppg_red", col("value"))).alias("red_avg"),
            stddev("value").alias("ppg_std")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    ppg_with_hr = ppg_agg.withColumn("hr_bpm", hr_from_ppg_udf(col("ppg_values"), col("ppg_ts")))

    spo2_dual = expr("least(100.0, greatest(85.0, 110.0 - 25.0 * (red_avg/ir_avg)))")
    spo2_fallback = expr("least(100.0, greatest(90.0, 95.0 + 5.0*(1 - (ppg_std / greatest(1e-6, abs(ppg_std)*10)))))")

    ppg_win = (
        ppg_with_hr
        .withColumn("spo2",
            when(col("ir_avg").isNotNull() & col("red_avg").isNotNull(), spo2_dual)
            .otherwise(spo2_fallback)
        )
        .withColumn(
            "stress_level",
            when(col("ppg_std") < 0.02, lit("low"))
            .when(col("ppg_std") < 0.06, lit("medium"))
            .otherwise(lit("high"))
        )
        .select("user_id","window_start","window_end","hr_bpm","spo2","stress_level")
    )

    # --- Accelerometro: steps proxy ---
    acc_win = (
        batch_df
        .filter(col("measurement_type") == "accelerometer")
        .withWatermark("datetime", "2 minutes")
        .groupBy(col("user_id"), window(col("datetime"), "1 minute"))
        .agg(
            stddev("value").alias("acc_std"),
            count("*").alias("acc_samples")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("step_count", expr("CAST(greatest(0.0, (acc_std - 0.08)) * 180 AS INT)"))
        .select("user_id","window_start","window_end","step_count")
    )

    # --- cEDA: calories proxy ---
    ceda_win = (
        batch_df
        .filter(col("measurement_type") == "ceda")
        .withWatermark("datetime", "2 minutes")
        .groupBy(col("user_id"), window(col("datetime"), "1 minute"))
        .agg(avg("value").alias("eda_avg"))
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("calories", expr("eda_avg * 0.1"))
        .select("user_id","window_start","window_end","calories")
    )

    # --- Join per finestra ---
    gold = (
        ppg_win.alias("p")
        .join(
            acc_win.alias("a"),
            (col("p.user_id")==col("a.user_id")) &
            (col("p.window_start")==col("a.window_start")) &
            (col("p.window_end")==col("a.window_end")),
            "left"
        )
        .join(
            ceda_win.alias("c"),
            (col("p.user_id")==col("c.user_id")) &
            (col("p.window_start")==col("c.window_start")) &
            (col("p.window_end")==col("c.window_end")),
            "left"
        )
        .select(
            col("p.user_id").alias("user_id"),
            col("p.window_start").alias("window_start"),
            col("p.window_end").alias("window_end"),
            col("p.hr_bpm").alias("hr_bpm"),
            col("p.spo2").alias("spo2"),
            col("a.step_count").alias("step_count"),
            col("p.stress_level").alias("stress_level"),
            col("c.calories").alias("calories")
        )
    )

    if not gold.take(1):
        print(f"⚠️ Batch {batch_id}: nessuna finestra chiusa")
        return

    # --- Scrittura PER-UTENTE ---
    users = [r.user_id for r in gold.select("user_id").distinct().collect()]
    for uid in users:
        data_u = gold.filter(col("user_id")==uid)
        if data_u.take(1):
            (
                data_u.coalesce(1)
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(f"s3a://gold/users/{uid}/metrics/")
            )
    print(f"✅ Batch {batch_id}: scritte tabelle per {len(users)} utenti")

# ========================
# Streaming
# ========================
query = (
    df_silver.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "s3a://gold/checkpoints/final_metrics_per_user/")
    .start()
)

print("🚀 Gold Layer minimale avviato — output per utente in s3a://gold/users/<user_id>/metrics/")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("⏹ Stop richiesto")
    query.stop()
    spark.stop()
