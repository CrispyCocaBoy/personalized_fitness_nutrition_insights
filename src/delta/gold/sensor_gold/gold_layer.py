# jobs/gold_metrics_with_daily_optimized.py
import os
import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, expr, current_timestamp, when, avg, stddev,
    collect_list, window, lit, to_date, coalesce,
    sum as Fsum, avg as Favg, min as Fmin, max as Fmax, count as Fcount
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, FloatType, IntegerType, BooleanType
)
from pyspark.sql.functions import udf
from delta.tables import DeltaTable

# ========================
# Config & paths
# ========================
S3_ENDPOINT     = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY   = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY   = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_GOLD  = os.getenv("S3_BUCKET_GOLD", "gold")
USERS_BASE      = f"s3a://{S3_BUCKET_GOLD}"
CKP_STREAM      = f"s3a://{S3_BUCKET_GOLD}/checkpoints/final_metrics_per_user/"
KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
SILVER_TOPIC    = os.getenv("SILVER_TOPIC", "silver_layer")
START_OFFSETS   = os.getenv("STARTING_OFFSETS", "latest")
FAIL_ON_LOSS    = os.getenv("FAIL_ON_DATA_LOSS", "false")
TRIGGER         = os.getenv("TRIGGER", "30 seconds")
MAX_OFFSETS     = os.getenv("MAX_OFFSETS_PER_TRIGGER", "")  # es. "50000"
USER_WEIGHT_KG  = float(os.getenv("USER_WEIGHT_KG", "75"))   # peso in kg usato nella formula kcal

FACT_PATH       = f"{USERS_BASE}/metrics_fact"       # partizioni: user_id, event_date
DAILY_PATH      = f"{USERS_BASE}/metrics_daily"      # chiave: (user_id, event_date)

# --------- filtro toggle da Redis ---------
SENSOR_TOGGLE_FILTER = os.getenv("SENSOR_TOGGLE_FILTER", "0").strip() == "1"
REDIS_HOST_TOGGLE    = os.getenv("REDIS_HOST_TOGGLE", "redis")
REDIS_PORT_TOGGLE    = int(os.getenv("REDIS_PORT_TOGGLE", "6379"))
REDIS_DB_TOGGLE      = int(os.getenv("REDIS_DB_TOGGLE", "0"))  # DB 0: sensor_id -> 0/1
REDIS_HASH_TOGGLE    = os.getenv("REDIS_HASH_TOGGLE", "")      # se usi HSET, es. "sensor_toggle"
REDIS_KEY_PREFIX     = os.getenv("REDIS_KEY_PREFIX", "sensor:")  # prefisso per chiavi sciolte
# -------------------------------------------------------------------

# ---- Fallback DB (CockroachDB / Postgres wire) ----
CRDB_HOST     = os.getenv("CRDB_HOST", "cockroachdb")
CRDB_PORT     = int(os.getenv("CRDB_PORT", "26257"))
CRDB_DBNAME   = os.getenv("CRDB_DBNAME", "user_device_db")
CRDB_USER     = os.getenv("CRDB_USER", "root")
CRDB_PASSWORD = os.getenv("CRDB_PASSWORD", "")            # se non usi password, lascia vuoto
CRDB_SSLMODE  = os.getenv("CRDB_SSLMODE", "disable")      # 'require' se cluster sicuro
CRDB_TIMEOUT  = int(os.getenv("CRDB_TIMEOUT", "5"))

# ========================
# Spark & Delta
# ========================
spark = (
    SparkSession.builder
    .appName("GoldLayer (Fact + Daily) — Optimized")
    .config("spark.sql.session.timeZone", "UTC")  # importante per le finestre
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Tuning parallelismo (cluster: 2 worker x 2 core = 4 core totali)
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    # Delta write compattanti (OSS compatibile)
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")
    # Limitazione dimensione file per ridurre small files
    .config("spark.sql.files.maxRecordsPerFile", "500000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# S3A / MinIO
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", S3_ACCESS_KEY)
hadoopConf.set("fs.s3a.secret.key", S3_SECRET_KEY)
hadoopConf.set("fs.s3a.endpoint", S3_ENDPOINT)
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# Tuning utili su MinIO
hadoopConf.set("fs.s3a.fast.upload", "true")
hadoopConf.set("fs.s3a.connection.maximum", "200")
hadoopConf.set("fs.s3a.threads.max", "200")
hadoopConf.set("fs.s3a.multipart.size", "134217728")  # 128MB
hadoopConf.set("fs.s3a.fast.upload.buffer", "disk")

print("✅ Spark ready & MinIO configured (UTC)")

# ========================
# Schema Silver
# ========================
silver_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", LongType()),            # epoch ms
    StructField("metric", StringType()),
    StructField("value", DoubleType()),
    StructField("topic", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("latency_sec", DoubleType()),
    StructField("measurement_type", StringType()),   # ppg / accelerometer / ceda
    StructField("user_id", StringType())
])

df_silver_base = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", SILVER_TOPIC)
    .option("startingOffsets", START_OFFSETS)
    .option("failOnDataLoss", FAIL_ON_LOSS)
)
if MAX_OFFSETS.strip():
    df_silver_base = df_silver_base.option("maxOffsetsPerTrigger", MAX_OFFSETS.strip())

df_silver = (
    df_silver_base.load()
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), silver_schema).alias("data"))
    .select("data.*")
    .withColumn("datetime", expr("from_unixtime(timestamp/1000)").cast("timestamp"))
)
# -------------------------------------------------------------------
# filtro sensori ON/OFF da Redis
# -------------------------------------------------------------------
_toggle_broadcast = None
_toggle_last_refresh = 0.0

def _load_toggle_map_from_redis():
    """
    Legge DB 0: mappa sensor_id -> '0'/'1'.
    - Se REDIS_HASH_TOGGLE è settato: HGETALL <hash>
    - Altrimenti: SCAN '<prefix>*' e GET per ogni chiave
    Se Redis non risponde o la mappa è vuota, usa il fallback su CockroachDB.
    """
    mapping = {}
    try:
        r = redis.StrictRedis(
            host=REDIS_HOST_TOGGLE, port=REDIS_PORT_TOGGLE,
            db=REDIS_DB_TOGGLE, decode_responses=True
        )
        if REDIS_HASH_TOGGLE:
            m = r.hgetall(REDIS_HASH_TOGGLE) or {}
            mapping = {k: ('1' if str(v) == '1' else '0') for k, v in m.items()}
        else:
            cursor = 0
            keys = []
            match = f"{REDIS_KEY_PREFIX}*"
            while True:
                cursor, batch = r.scan(cursor=cursor, match=match, count=1000)
                keys.extend(batch)
                if cursor == 0:
                    break
            if keys:
                pipe = r.pipeline()
                for k in keys:
                    pipe.get(k)
                vals = pipe.execute()
                pref_len = len(REDIS_KEY_PREFIX)
                mapping = {
                    k[pref_len:]: ('1' if str(v) == '1' else '0')
                    for k, v in zip(keys, vals) if v is not None
                }
    except Exception as e:
        print(f"⚠️ Redis non disponibile: {e}")
        mapping = {}

    # ---- FALLBACK: se mapping vuoto, prova CockroachDB ----
    if not mapping:
        db_map = _load_toggle_map_from_db()
        if db_map:
            print(f"ℹ️ Toggle map caricata da CockroachDB: {len(db_map)} righe")
            return db_map

    return mapping


# fallback -> usare in caso di emergenza
def _load_toggle_map_from_db():
    """
    Fallback: legge la tabella sensor_status da CockroachDB e
    ritorna dict[str, '0'|'1'] in base al campo 'active'.
    """
    try:
        # DSN-style; sslmode e timeout sono utili per non bloccare lo stream
        dsn = (
            f"host={CRDB_HOST} port={CRDB_PORT} dbname={CRDB_DBNAME} "
            f"user={CRDB_USER} sslmode={CRDB_SSLMODE} connect_timeout={CRDB_TIMEOUT}"
        )
        if CRDB_PASSWORD:
            dsn += f" password={CRDB_PASSWORD}"

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                # Prendiamo tutto: se vuoi solo attivi, cambia in WHERE active = true
                cur.execute("SELECT sensor_id, active FROM sensor_status")
                rows = cur.fetchall()  # [(sensor_id, active_bool), ...]
                mapping = {str(r[0]): ('1' if bool(r[1]) else '0') for r in rows}
                return mapping
    except Exception as e:
        # Silenzioso: se fallisce anche il DB, restituisco {} -> no-op
        print(f"⚠️ Fallback DB sensor_status non disponibile: {e}")
        return {}


def _ensure_toggle_broadcast(force=False, ttl_sec=300):

    global _toggle_broadcast, _toggle_last_refresh
    if not SENSOR_TOGGLE_FILTER:
        return _toggle_broadcast  # dormiente
    now = time.time()
    if not force and (now - _toggle_last_refresh) < ttl_sec and _toggle_broadcast is not None:
        return _toggle_broadcast
    m = _load_toggle_map_from_redis()
    try:
        if _toggle_broadcast is not None:
            _toggle_broadcast.unpersist(blocking=False)
            _toggle_broadcast.destroy()
    except Exception:
        pass
    _toggle_broadcast = spark.sparkContext.broadcast(m)
    _toggle_last_refresh = now
    return _toggle_broadcast

def _is_sensor_on_udf_factory():

    def _inner(sensor_id):
        if not SENSOR_TOGGLE_FILTER:
            return True
        try:
            m = _toggle_broadcast.value if _toggle_broadcast is not None else {}
        except Exception:
            m = {}
        if not m:
            return True
        sid = str(sensor_id) if sensor_id is not None else ""
        return m.get(sid, '1') == '1'  # default ON
    return udf(_inner, BooleanType())

_is_sensor_on = _is_sensor_on_udf_factory()

def apply_sensor_toggle_filter(df):
    _ensure_toggle_broadcast(force=False, ttl_sec=1)  # soft-refresh
    # Se flag OFF o mappa vuota -> no-op
    try:
        m = _toggle_broadcast.value if _toggle_broadcast is not None else {}
    except Exception:
        m = {}
    if not SENSOR_TOGGLE_FILTER or not m:
        return df
    return df.withColumn("_on", _is_sensor_on(col("sensor_id"))) \
             .filter(col("_on")) \
             .drop("_on")
# -------------------------------------------------------------------

# ========================
# UDF PPG: HR (bpm) + HRV (ms)
# ========================
def _hr_hrv_from_ppg(values, timestamps_ms):
    try:
        import numpy as np
        if not values or not timestamps_ms or len(values) < 20:
            return (None, None)
        v = np.asarray(values, dtype=float)
        t = np.asarray(timestamps_ms, dtype=float)

        # robust detrend/scale
        v = v - np.median(v)
        mad = np.median(np.abs(v)) + 1e-8
        v = v / (mad * 6.0)

        dt = np.diff(t); dt = dt[dt > 0]
        if dt.size == 0:
            return (None, None)
        fs = 1000.0 / float(np.median(dt))
        thr = float(np.mean(v) + 0.4*np.std(v))
        min_rr_n = max(1, int(0.30 * fs))

        peaks, last = [], -10**9
        for i in range(1, len(v)-1):
            if v[i] > thr and v[i] > v[i-1] and v[i] >= v[i+1]:
                if i - last >= min_rr_n:
                    peaks.append(i); last = i
        if len(peaks) < 2:
            return (None, None)

        rr = []
        for k in range(1, len(peaks)):
            dtms = t[peaks[k]] - t[peaks[k-1]]
            if 300.0 < dtms < 2000.0:
                rr.append(dtms)
        if len(rr) < 2:
            return (None, None)

        import numpy as _np
        rr = _np.asarray(rr, dtype=float)
        hr_raw = 60000.0 / float(_np.mean(rr))
        hrv_ms = float(_np.std(rr, ddof=1))  # SDNN (ms)

        # clamp HR plausibile (correttivo +20 applicato dopo)
        hr_raw = float(_np.clip(hr_raw, 35.0, 210.0))
        return (hr_raw, hrv_ms)
    except Exception:
        return (None, None)

from pyspark.sql.types import StructType as _Struct, StructField as _Field
hr_struct = _Struct([
    _Field("hr_bpm_raw", FloatType()),
    _Field("hrv_ms", FloatType())
])
hr_hrv_udf = udf(_hr_hrv_from_ppg, hr_struct)

# ========================
# ForeachBatch: FACT + DAILY
# ========================
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"⚠️ Batch {batch_id} vuoto")
        return

    # --- PPG: HR/HRV + SPO2 proxy + Stress ---
    ppg_agg = (
        batch_df
        .filter(col("measurement_type") == "ppg")
        .withWatermark("datetime", "2 minutes")
        .groupBy(col("user_id"), window(col("datetime"), "1 minute"))
        .agg(
            collect_list("value").alias("ppg_values"),
            collect_list("timestamp").alias("ppg_ts"),
            avg(when(col("metric") == "ppg_ir",  col("value"))).alias("ir_avg"),
            avg(when(col("metric") == "ppg_red", col("value"))).alias("red_avg"),
            stddev("value").alias("ppg_std")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    # HR + HRV
    ppg_with_hr = ppg_agg.withColumn("hr_pack", hr_hrv_udf(col("ppg_values"), col("ppg_ts")))
    ppg_with_hr = (
        ppg_with_hr
        .withColumn("hr_bpm_raw", col("hr_pack.hr_bpm_raw"))
        .withColumn("hrv_ms", col("hr_pack.hrv_ms"))
        .drop("hr_pack")
        .withColumn("hr_bpm", when(col("hr_bpm_raw").isNotNull(), col("hr_bpm_raw") + lit(20.0)).otherwise(None))  # correttivo +20
    )

    # SPO2 proxy
    spo2_dual = expr("least(100.0, greatest(85.0, 110.0 - 25.0 * (red_avg/ir_avg)))")
    spo2_fallback = expr("least(100.0, greatest(90.0, 95.0 + 5.0*(1 - (ppg_std / greatest(1e-6, abs(ppg_std)*10)))))")

    # Stress: HRV-first, fallback PPG std
    stress_by_hrv = (
        when(col("hrv_ms").isNotNull() & (col("hrv_ms") <= 20), lit("high"))      # molto alta variabilità = stress alto
        .when(col("hrv_ms").isNotNull() & (col("hrv_ms") <= 35), lit("medium"))   # media variabilità = stress medio  
        .when(col("hrv_ms").isNotNull() & (col("hrv_ms") <= 50), lit("medium"))   # normale variabilità = stress medio
        .when(col("hrv_ms").isNotNull(), lit("low"))                              # alta variabilità = stress basso
        .otherwise(lit(None))
    )
    stress_fallback_ppg = (
        when(col("ppg_std") < 0.01, lit("low"))      # soglia più bassa per low stress
        .when(col("ppg_std") < 0.035, lit("medium")) # soglia più bassa per medium stress
        .otherwise(lit("high"))
    )

    ppg_win = (
        ppg_with_hr
        .withColumn("spo2",
            when(col("ir_avg").isNotNull() & col("red_avg").isNotNull(), spo2_dual)
            .otherwise(spo2_fallback)
        )
        .withColumn(
            "stress_level",
            when(stress_by_hrv.isNotNull(), stress_by_hrv).otherwise(stress_fallback_ppg)
        )
        .select("user_id","window_start","window_end","hr_bpm","hrv_ms","spo2","stress_level","ppg_std")
    )

    # --- Accelerometro: steps proxy ---
    acc_win = (
        batch_df
        .filter(col("measurement_type") == "accelerometer")
        .withWatermark("datetime", "2 minutes")
        .groupBy(col("user_id"), window(col("datetime"), "1 minute"))
        .agg(
            stddev("value").alias("acc_std"),
            Fcount("*").alias("acc_samples")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("step_count", expr("CAST(greatest(0.0, (acc_std - 0.08)) * 180 AS INT)"))
        .select("user_id","window_start","window_end","step_count")
    )

    # --- Join per finestra (ppg base + acc) ---
    base = (
        ppg_win.alias("p")
        .join(
            acc_win.alias("a"),
            (col("p.user_id")==col("a.user_id")) &
            (col("p.window_start")==col("a.window_start")) &
            (col("p.window_end")==col("a.window_end")),
            "left"
        )
        .select(
            col("p.user_id").alias("user_id"),
            col("p.window_start").alias("window_start"),
            col("p.window_end").alias("window_end"),
            col("p.hr_bpm").alias("hr_bpm"),
            col("p.hrv_ms").alias("hrv_ms"),
            col("p.spo2").alias("spo2"),
            col("a.step_count").alias("step_count"),
            col("p.stress_level").alias("stress_level")
        )
    )

    # --- Calorie (kcal) con formula richiesta ---
    # KCAL_T = T * peso * 0.85 * (25 + HRV/6 + (BPM-60)/12) / 24 + passi * 0.03
    # T in ore per finestra di 1 minuto = ~1/60 h (calcolato da window_end/start per robustezza)
    gold = (
        base
        .withColumn("event_date", to_date(col("window_start")))
        .withColumn("T_hours", (expr("unix_timestamp(window_end) - unix_timestamp(window_start)"))/lit(3600.0))
        .withColumn("weight", lit(USER_WEIGHT_KG))
        .withColumn("hrv_used", coalesce(col("hrv_ms"), lit(50.0)))   # fallback ragionevole
        .withColumn("bpm_used", coalesce(col("hr_bpm"), lit(70.0)))
        .withColumn("steps_used", coalesce(col("step_count"), lit(0)))
        .withColumn(
            "calories",
            expr("""
                T_hours * weight * 0.85 * (25 + hrv_used/6.0 + (bpm_used - 60)/12.0) / 24.0
            """) + col("steps_used") * lit(0.03)
        )
        .withColumn("gold_ingest_ts", current_timestamp())
        .select("user_id","window_start","window_end","event_date",
                "hr_bpm","hrv_ms","spo2","step_count","stress_level","calories","gold_ingest_ts")
    )

    if gold.rdd.isEmpty():
        print(f"⚠️ Batch {batch_id}: nessuna finestra chiusa")
        return

    # ===========
    # CAST (stabilizza schema)
    # ===========
    gold_cast = (
        gold
        .withColumn("hr_bpm",     col("hr_bpm").cast(DoubleType()))
        .withColumn("hrv_ms",     col("hrv_ms").cast(DoubleType()))
        .withColumn("spo2",       col("spo2").cast(DoubleType()))
        .withColumn("step_count", col("step_count").cast(IntegerType()))
        .withColumn("calories",   col("calories").cast(DoubleType()))
    )

    # ===========
    # FACT (append coerente)
    # ===========
    (gold_cast.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .partitionBy("user_id", "event_date")
         .save(FACT_PATH))

    # ===========
    # DAILY (build dal gold_cast)
    # ===========
    daily = (
        gold_cast.groupBy("user_id", "event_date")
            .agg(
                Favg("hr_bpm").cast(DoubleType()).alias("hr_bpm_avg"),
                Fmin("hr_bpm").cast(DoubleType()).alias("hr_bpm_min"),
                Fmax("hr_bpm").cast(DoubleType()).alias("hr_bpm_max"),
                Favg("spo2").cast(DoubleType()).alias("spo2_avg"),

                Fsum(coalesce(col("step_count"), lit(0))).cast(IntegerType()).alias("steps_total"),
                Fsum(coalesce(col("calories"), lit(0.0))).cast(DoubleType()).alias("calories_total"),

                Fsum(when(col("stress_level")=="low",    1).otherwise(0)).cast(IntegerType()).alias("stress_low_cnt"),
                Fsum(when(col("stress_level")=="medium", 1).otherwise(0)).cast(IntegerType()).alias("stress_medium_cnt"),
                Fsum(when(col("stress_level")=="high",   1).otherwise(0)).cast(IntegerType()).alias("stress_high_cnt"),

                Fcount(lit(1)).cast(IntegerType()).alias("windows_count"),
                Fmax("window_end").alias("last_window_end")
            )
            .withColumn("dominant_stress",
                expr("""
                    case
                      when stress_high_cnt >= greatest(stress_medium_cnt, stress_low_cnt) then 'high'
                      when stress_medium_cnt >= greatest(stress_high_cnt, stress_low_cnt) then 'medium'
                      else 'low'
                    end
                """)
            )
            .withColumn("gold_ingest_ts", current_timestamp())
    )

    # ===========
    # MERGE incrementale sulla DAILY
    # ===========
    if DeltaTable.isDeltaTable(spark, DAILY_PATH):
        tgt = DeltaTable.forPath(spark, DAILY_PATH)
        (tgt.alias("t")
            .merge(
                daily.alias("s"),
                "t.user_id = s.user_id AND t.event_date = s.event_date"
            )
            # Update incrementale: somme per cumulativi, medie ponderate per avg
            .whenMatchedUpdate(set = {
                # medie ponderate (evita nullif per compat): denom = t.windows_count + s.windows_count
                "hr_bpm_avg": expr("""
                    CASE WHEN (t.windows_count + s.windows_count) = 0
                         THEN COALESCE(s.hr_bpm_avg, t.hr_bpm_avg)
                         ELSE (COALESCE(t.hr_bpm_avg,0) * t.windows_count + COALESCE(s.hr_bpm_avg,0) * s.windows_count)
                              / (t.windows_count + s.windows_count)
                    END
                """),
                "hr_bpm_min": expr("least(COALESCE(t.hr_bpm_min, 999), COALESCE(s.hr_bpm_min, 999))"),
                "hr_bpm_max": expr("greatest(COALESCE(t.hr_bpm_max, 0), COALESCE(s.hr_bpm_max, 0))"),
                "spo2_avg": expr("""
                    CASE WHEN (t.windows_count + s.windows_count) = 0
                         THEN COALESCE(s.spo2_avg, t.spo2_avg)
                         ELSE (COALESCE(t.spo2_avg,0) * t.windows_count + COALESCE(s.spo2_avg,0) * s.windows_count)
                              / (t.windows_count + s.windows_count)
                    END
                """),

                # FIX: passi devono essere INCREMENTATI, non sostituiti
                "steps_total":     expr("COALESCE(t.steps_total, 0) + COALESCE(s.steps_total, 0)"),
                "calories_total":  expr("COALESCE(t.calories_total, 0) + COALESCE(s.calories_total, 0)"),

                "stress_low_cnt":    expr("COALESCE(t.stress_low_cnt, 0) + COALESCE(s.stress_low_cnt, 0)"),
                "stress_medium_cnt": expr("COALESCE(t.stress_medium_cnt, 0) + COALESCE(s.stress_medium_cnt, 0)"),
                "stress_high_cnt":   expr("COALESCE(t.stress_high_cnt, 0) + COALESCE(s.stress_high_cnt, 0)"),

                "windows_count":   expr("COALESCE(t.windows_count, 0) + COALESCE(s.windows_count, 0)"),
                "last_window_end": expr("greatest(COALESCE(t.last_window_end, s.last_window_end), COALESCE(s.last_window_end, t.last_window_end))"),

                # Ricalcolo dominant_stress dopo l'incremento
                "dominant_stress": expr("""
                    case
                      when (COALESCE(t.stress_high_cnt,0) + COALESCE(s.stress_high_cnt,0)) >= 
                           greatest(COALESCE(t.stress_medium_cnt,0) + COALESCE(s.stress_medium_cnt,0),
                                   COALESCE(t.stress_low_cnt,0) + COALESCE(s.stress_low_cnt,0)) then 'high'
                      when (COALESCE(t.stress_medium_cnt,0) + COALESCE(s.stress_medium_cnt,0)) >= 
                           greatest(COALESCE(t.stress_high_cnt,0) + COALESCE(s.stress_high_cnt,0),
                                   COALESCE(t.stress_low_cnt,0) + COALESCE(s.stress_low_cnt,0)) then 'medium'
                      else 'low'
                    end
                """),
                "gold_ingest_ts": current_timestamp()
            })
            .whenNotMatchedInsertAll()
            .execute())
    else:
        (daily.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .save(DAILY_PATH))

    print(f"✅ Batch {batch_id}: wrote FACT → {FACT_PATH} | merged DAILY → {DAILY_PATH}")

# ========================
# Streaming query
# ========================
query_builder = (
    df_silver.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")  # richiesto dall’API
    .trigger(processingTime=TRIGGER)
    .option("checkpointLocation", CKP_STREAM)
)

query = query_builder.start()
print(f"🚀 Gold (fact + daily) avviato — FACT: {FACT_PATH} | DAILY: {DAILY_PATH}")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("⏹ Stop richiesto")
    query.stop()
    spark.stop()