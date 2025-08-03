from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, expr, when, to_json, struct,
    window, avg, max, min, stddev, count, sum as spark_sum,
    percentile_approx, lag, lead, abs as spark_abs, sqrt, pow as spark_pow,
    collect_list, sort_array, size, slice, element_at, desc, asc,
    first, last, var_pop, skewness, kurtosis, corr, covar_pop, variance,
    array, concat, lit, explode, posexplode, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField, ArrayType, FloatType, \
    IntegerType
from pyspark.sql.window import Window
import math

# SparkSession
spark = (
    SparkSession.builder
    .appName("goldLayerAdvancedHealthMetrics")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session created for Advanced gold Layer")

# MinIO Configuration
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
print("✅ MinIO configured")

# =========================================
# Schema per Silver Layer Data
# =========================================
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

# Lettura dal topic silver_layer
df_silver = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker_kafka:9092")
    .option("subscribe", "silver_layer")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as json", "CAST(key AS STRING) as user_id_key")
    .select(from_json(col("json"), silver_schema).alias("data"))
    .select("data.*")
    .withColumn("processing_ts", current_timestamp())
    .withColumn("datetime", expr("from_unixtime(timestamp/1000)").cast("timestamp"))
)

# =========================================
# UDF per calcoli avanzati PPG
# =========================================
from pyspark.sql.functions import udf


# Heart Rate e HRV avanzati
@udf(returnType=StructType([
    StructField("hr_bpm", FloatType()),
    StructField("hrv_sdnn", FloatType()),
    StructField("hrv_rmssd", FloatType()),
    StructField("stress_index", FloatType()),
    StructField("spo2_estimate", FloatType())
]))
def advanced_ppg_metrics(values, timestamps):
    """Calcola metriche PPG avanzate"""
    if not values or len(values) < 30:  # Almeno 30 campioni per 1 minuto
        return (None, None, None, None, None)

    import numpy as np

    # Heart Rate da peak detection
    peaks = []
    threshold = np.mean(values) + 0.5 * np.std(values)
    for i in range(1, len(values) - 1):
        if values[i] > values[i - 1] and values[i] > values[i + 1] and values[i] > threshold:
            peaks.append(i)

    if len(peaks) < 2:
        return (None, None, None, None, None)

    # Calcola RR intervals (in ms, assumendo 50Hz sampling)
    rr_intervals = []
    for i in range(1, len(peaks)):
        rr_ms = (peaks[i] - peaks[i - 1]) * 20  # 20ms per sample a 50Hz
        if 300 < rr_ms < 2000:  # Filtro valori realistici
            rr_intervals.append(rr_ms)

    if len(rr_intervals) < 2:
        return (None, None, None, None, None)

    # Heart Rate
    hr = 60000 / np.mean(rr_intervals) if rr_intervals else None
    hr = max(40, min(200, hr)) if hr else None

    # HRV SDNN (standard deviation of NN intervals)
    sdnn = np.std(rr_intervals) if len(rr_intervals) > 1 else None

    # HRV RMSSD (root mean square of successive differences)
    if len(rr_intervals) > 1:
        diffs = [abs(rr_intervals[i] - rr_intervals[i - 1]) for i in range(1, len(rr_intervals))]
        rmssd = np.sqrt(np.mean([d ** 2 for d in diffs])) if diffs else None
    else:
        rmssd = None

    # Stress Index (Baevsky's triangular index)
    stress_idx = None
    if sdnn and len(rr_intervals) > 10:
        stress_idx = len(rr_intervals) / (2 * max(np.max(rr_intervals) - np.min(rr_intervals), 1))

    # SpO2 estimate (semplificato basato su variabilità segnale)
    signal_quality = 1 - (np.std(values) / max(np.mean(values), 1))
    spo2 = 95 + (signal_quality * 5) if signal_quality > 0.3 else None
    spo2 = max(85, min(100, spo2)) if spo2 else None

    return (float(hr) if hr else None,
            float(sdnn) if sdnn else None,
            float(rmssd) if rmssd else None,
            float(stress_idx) if stress_idx else None,
            float(spo2) if spo2 else None)


# Metriche temperatura avanzate
@udf(returnType=StructType([
    StructField("avg_temp", FloatType()),
    StructField("temp_trend", StringType()),
    StructField("rapid_changes", IntegerType())
]))
def advanced_temp_metrics(values):
    """Calcola metriche temperatura avanzate"""
    if not values or len(values) < 10:
        return (None, None, None)

    import numpy as np

    avg_temp = float(np.mean(values))

    # Trend detection usando regressione lineare semplice
    x = list(range(len(values)))
    slope = np.polyfit(x, values, 1)[0] if len(values) > 1 else 0

    if slope > 0.01:
        trend = "increasing"
    elif slope < -0.01:
        trend = "decreasing"
    else:
        trend = "stable"

    # Rapid changes detection (cambio > 0.5°C in 10 secondi)
    rapid_changes = 0
    for i in range(1, len(values)):
        if abs(values[i] - values[i - 1]) > 0.5:
            rapid_changes += 1

    return (avg_temp, trend, rapid_changes)


# Metriche accelerometro avanzate
@udf(returnType=StructType([
    StructField("step_count", IntegerType()),
    StructField("cadence", FloatType()),
    StructField("vector_magnitude", FloatType()),
    StructField("active_time_sec", FloatType()),
    StructField("sedentary_time_sec", FloatType())
]))
def advanced_accel_metrics(values):
    """Calcola metriche accelerometro avanzate"""
    if not values or len(values) < 20:
        return (None, None, None, None, None)

    import numpy as np

    # Step counting con peak detection migliorato
    # Calcola Vector Magnitude
    vm_mean = np.mean(values)
    vm_std = np.std(values)
    vector_magnitude = float(vm_std)

    # Step detection
    threshold = vm_mean + 0.5 * vm_std
    steps = 0
    last_step = 0

    for i in range(1, len(values) - 1):
        if (values[i] > values[i - 1] and values[i] > values[i + 1] and
                values[i] > threshold and i - last_step > 10):  # Almeno 10 campioni tra passi
            steps += 1
            last_step = i

    # Cadence (passi per minuto)
    cadence = steps * (60.0 / (len(values) / 50.0)) if steps > 0 else 0.0  # Assumendo 50Hz

    # Active vs Sedentary time
    active_threshold = vm_mean + 0.3 * vm_std
    active_samples = sum(1 for v in values if v > active_threshold)
    sedentary_samples = len(values) - active_samples

    # Converti in secondi (assumendo 50Hz)
    active_time = active_samples / 50.0
    sedentary_time = sedentary_samples / 50.0

    return (steps, float(cadence), vector_magnitude, float(active_time), float(sedentary_time))


# Metriche giroscopio avanzate
@udf(returnType=StructType([
    StructField("avg_angular_velocity", FloatType()),
    StructField("posture_changes", IntegerType()),
    StructField("fall_risk_score", FloatType())
]))
def advanced_gyro_metrics(values):
    """Calcola metriche giroscopio avanzate"""
    if not values or len(values) < 10:
        return (None, None, None)

    import numpy as np

    avg_angular_vel = float(np.mean(np.abs(values)))

    # Posture changes (grandi variazioni nella velocità angolare)
    posture_changes = 0
    threshold = np.std(values) * 2

    for i in range(1, len(values)):
        if abs(values[i] - values[i - 1]) > threshold:
            posture_changes += 1

    # Fall risk score basato su variabilità
    fall_risk = float(np.std(values) * np.max(np.abs(values)))
    fall_risk = min(10.0, fall_risk)  # Scala 0-10

    return (avg_angular_vel, posture_changes, fall_risk)


# Metriche altimetro avanzate
@udf(returnType=StructType([
    StructField("cumulative_ascent", FloatType()),
    StructField("cumulative_descent", FloatType()),
    StructField("avg_altitude", FloatType()),
    StructField("avg_slope", FloatType())
]))
def advanced_altitude_metrics(values):
    """Calcola metriche altitudine avanzate"""
    if not values or len(values) < 5:
        return (None, None, None, None)

    import numpy as np

    avg_alt = float(np.mean(values))

    # Calcola ascent e descent cumulativi
    ascent = 0.0
    descent = 0.0

    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        if diff > 0.5:  # Soglia per eliminare rumore
            ascent += diff
        elif diff < -0.5:
            descent += abs(diff)

    # Slope medio (pendenza)
    if len(values) > 1:
        total_distance = len(values) * 2.0  # Assumendo 2m per campione
        elevation_change = values[-1] - values[0]
        avg_slope = float(elevation_change / total_distance * 100)  # Percentuale
    else:
        avg_slope = 0.0

    return (float(ascent), float(descent), avg_alt, avg_slope)


# Metriche barometro avanzate
@udf(returnType=StructType([
    StructField("pressure_trend", StringType()),
    StructField("estimated_altitude", FloatType())
]))
def advanced_pressure_metrics(values):
    """Calcola metriche pressione avanzate"""
    if not values or len(values) < 5:
        return (None, None)

    import numpy as np

    # Trend detection
    x = list(range(len(values)))
    slope = np.polyfit(x, values, 1)[0] if len(values) > 1 else 0

    if slope > 0.1:
        trend = "rising"
    elif slope < -0.1:
        trend = "falling"
    else:
        trend = "stable"

    # Stima altitudine dalla pressione (formula barometrica)
    avg_pressure = np.mean(values)
    # Altitudine = 44330 * (1 - (P/P0)^(1/5.255))
    sea_level_pressure = 1013.25
    estimated_alt = 44330 * (1 - (avg_pressure / sea_level_pressure) ** (1 / 5.255))

    return (trend, float(estimated_alt))


# Metriche CEDA avanzate (Energy Expenditure e METs)
@udf(returnType=StructType([
    StructField("energy_expenditure_cal", FloatType()),
    StructField("mets", FloatType()),
    StructField("activity_class", StringType())
]))
def advanced_ceda_metrics(values, accel_values=None):
    """Calcola energia spesa e METs"""
    if not values or len(values) < 10:
        return (None, None, None)

    import numpy as np

    # Energy expenditure basato su EDA e movimento
    eda_mean = np.mean(values)
    eda_std = np.std(values)

    # Stima grossolana delle calorie (basata su arousal elettrodermico)
    # Formula semplificata: arousal correlato con metabolismo
    base_met = 1.2  # METs a riposo
    arousal_factor = min(eda_std / max(eda_mean, 0.1), 5.0)

    # Se abbiamo dati accelerometro, usali per migliorare la stima
    if accel_values and len(accel_values) > 0:
        movement_factor = min(np.std(accel_values), 3.0)
        mets = base_met + arousal_factor * 0.5 + movement_factor * 0.8
    else:
        mets = base_met + arousal_factor * 0.3

    mets = max(1.0, min(12.0, mets))  # Clamp tra 1-12 METs

    # Calorie per minuto (assumendo 70kg di peso medio)
    weight_kg = 70.0
    cal_per_min = mets * weight_kg * 3.5 / 200
    energy_expenditure = float(cal_per_min)

    # Classificazione attività basata su METs
    if mets < 1.5:
        activity_class = "sleep_rest"
    elif mets < 3.0:
        activity_class = "sedentary"
    elif mets < 6.0:
        activity_class = "light_activity"
    elif mets < 9.0:
        activity_class = "moderate_activity"
    else:
        activity_class = "vigorous_activity"

    return (energy_expenditure, float(mets), activity_class)


# =========================================
# Funzioni per l'invio a Kafka
# =========================================
def send_to_kafka(df, sensor_type, topic="gold_layer"):
    """Invia DataFrame a Kafka con formato strutturato"""
    return (
        df
        .withColumn("layer", lit("gold"))
        .withColumn("sensor_type", lit(sensor_type))
        .withColumn("processing_timestamp", current_timestamp())
        .select(
            col("user_id").cast("string").alias("key"),
            to_json(struct(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("layer"),
                col("sensor_type"),
                col("processing_timestamp"),
                struct("*").alias("metrics")
            )).alias("value")
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker_kafka:9092")
        .option("topic", topic)
        .save()
    )


def send_to_kafka_stream(df, sensor_type, topic="gold_layer"):
    """Versione streaming per l'invio a Kafka"""
    return (
        df
        .withColumn("layer", lit("gold"))
        .withColumn("sensor_type", lit(sensor_type))
        .withColumn("processing_timestamp", current_timestamp())
        .select(
            col("user_id").cast("string").alias("key"),
            to_json(struct(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("layer"),
                col("sensor_type"),
                col("processing_timestamp"),
                struct("*").alias("metrics")
            )).alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker_kafka:9092")
        .option("topic", topic)
        .option("checkpointLocation", f"s3a://gold/checkpoints/kafka_{sensor_type}/")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
    )


# =========================================
# Funzione principale per calcolare metriche avanzate
# =========================================
def calculate_advanced_gold_metrics(batch_df, batch_id):
    if not batch_df.take(1):
        print(f"⚠️ Batch {batch_id} vuoto")
        return

    print(f"🔄 Processing advanced metrics for batch {batch_id}...")

    # Finestra temporale di 1 minuto per ogni utente
    windowed_df = (
        batch_df
        .withWatermark("datetime", "2 minutes")
        .groupBy(
            col("user_id"),
            window(col("datetime"), "1 minute"),
            col("measurement_type")
        )
        .agg(
            count("*").alias("sample_count"),
            avg("value").alias("avg_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value"),
            stddev("value").alias("std_value"),
            collect_list("value").alias("raw_values"),
            collect_list("timestamp").alias("timestamps"),
            first("timestamp").alias("window_start_ts"),
            last("timestamp").alias("window_end_ts")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .filter(col("sample_count") >= 10)  # Almeno 10 campioni per finestra
    )

    # Separa i sensori
    sensors = {
        'ppg': windowed_df.filter(col("measurement_type") == "ppg"),
        'skin_temp': windowed_df.filter(col("measurement_type") == "skin_temp"),
        'accel': windowed_df.filter(col("measurement_type") == "accelerometer"),
        'gyro': windowed_df.filter(col("measurement_type") == "gyroscope"),
        'altitude': windowed_df.filter(col("measurement_type") == "altimeter"),
        'pressure': windowed_df.filter(col("measurement_type") == "barometer"),
        'ceda': windowed_df.filter(col("measurement_type") == "ceda")
    }

    # =========================================
    # CALCOLA METRICHE AVANZATE PER SENSORE
    # =========================================

    # PPG Metrics
    if sensors['ppg'].take(1):
        ppg_advanced = (
            sensors['ppg']
            .withColumn("ppg_metrics", advanced_ppg_metrics(col("raw_values"), col("timestamps")))
            .select(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("ppg_metrics.hr_bpm").alias("heart_rate_bpm"),
                col("ppg_metrics.hrv_sdnn").alias("hrv_sdnn_ms"),
                col("ppg_metrics.hrv_rmssd").alias("hrv_rmssd_ms"),
                col("ppg_metrics.stress_index").alias("stress_index"),
                col("ppg_metrics.spo2_estimate").alias("spo2_percent"),
                col("sample_count").alias("ppg_samples")
            )
        )

        # Salva su MinIO per ogni user E invia a Kafka
        user_ids = [row.user_id for row in ppg_advanced.select("user_id").distinct().collect()]
        for user_id in user_ids:
            user_data = ppg_advanced.filter(col("user_id") == user_id)
            if user_data.take(1):
                # Salva su MinIO
                (user_data.coalesce(1).write.format("delta").mode("append")
                 .option("mergeSchema", "true")
                 .save(f"s3a://gold/users/{user_id}/ppg_advanced/"))

                # Invia a Kafka
                try:
                    send_to_kafka(user_data, "ppg_advanced")
                    print(f"📤 PPG data sent to Kafka for user {user_id}")
                except Exception as e:
                    print(f"⚠️ Error sending PPG data to Kafka for user {user_id}: {e}")

        print(f"💓 Saved and sent PPG metrics for {len(user_ids)} users")

    # Skin Temperature Metrics
    if sensors['skin_temp'].take(1):
        temp_advanced = (
            sensors['skin_temp']
            .withColumn("temp_metrics", advanced_temp_metrics(col("raw_values")))
            .select(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("temp_metrics.avg_temp").alias("avg_temp_celsius"),
                col("temp_metrics.temp_trend").alias("temperature_trend"),
                col("temp_metrics.rapid_changes").alias("rapid_temp_changes"),
                col("min_value").alias("min_temp"),
                col("max_value").alias("max_temp")
            )
        )

        user_ids = [row.user_id for row in temp_advanced.select("user_id").distinct().collect()]
        for user_id in user_ids:
            user_data = temp_advanced.filter(col("user_id") == user_id)
            if user_data.take(1):
                # Salva su MinIO
                (user_data.coalesce(1).write.format("delta").mode("append")
                 .option("mergeSchema", "true")
                 .save(f"s3a://gold/users/{user_id}/temperature_advanced/"))

                # Invia a Kafka
                try:
                    send_to_kafka(user_data, "temperature_advanced")
                    print(f"📤 Temperature data sent to Kafka for user {user_id}")
                except Exception as e:
                    print(f"⚠️ Error sending temperature data to Kafka for user {user_id}: {e}")

        print(f"🌡️ Saved and sent temperature metrics for {len(user_ids)} users")

    # Accelerometer Metrics
    if sensors['accel'].take(1):
        accel_advanced = (
            sensors['accel']
            .withColumn("accel_metrics", advanced_accel_metrics(col("raw_values")))
            .select(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("accel_metrics.step_count").alias("step_count"),
                col("accel_metrics.cadence").alias("cadence_spm"),
                col("accel_metrics.vector_magnitude").alias("vector_magnitude"),
                col("accel_metrics.active_time_sec").alias("active_time_seconds"),
                col("accel_metrics.sedentary_time_sec").alias("sedentary_time_seconds")
            )
        )

        user_ids = [row.user_id for row in accel_advanced.select("user_id").distinct().collect()]
        for user_id in user_ids:
            user_data = accel_advanced.filter(col("user_id") == user_id)
            if user_data.take(1):
                # Salva su MinIO
                (user_data.coalesce(1).write.format("delta").mode("append")
                 .option("mergeSchema", "true")
                 .save(f"s3a://gold/users/{user_id}/accelerometer_advanced/"))

                # Invia a Kafka
                try:
                    send_to_kafka(user_data, "accelerometer_advanced")
                    print(f"📤 Accelerometer data sent to Kafka for user {user_id}")
                except Exception as e:
                    print(f"⚠️ Error sending accelerometer data to Kafka for user {user_id}: {e}")

        print(f"🚶 Saved and sent accelerometer metrics for {len(user_ids)} users")

    # Gyroscope Metrics
    if sensors['gyro'].take(1):
        gyro_advanced = (
            sensors['gyro']
            .withColumn("gyro_metrics", advanced_gyro_metrics(col("raw_values")))
            .select(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("gyro_metrics.avg_angular_velocity").alias("avg_angular_velocity"),
                col("gyro_metrics.posture_changes").alias("posture_changes"),
                col("gyro_metrics.fall_risk_score").alias("fall_risk_score")
            )
        )

        user_ids = [row.user_id for row in gyro_advanced.select("user_id").distinct().collect()]
        for user_id in user_ids:
            user_data = gyro_advanced.filter(col("user_id") == user_id)
            if user_data.take(1):
                # Salva su MinIO
                (user_data.coalesce(1).write.format("delta").mode("append")
                 .option("mergeSchema", "true")
                 .save(f"s3a://gold/users/{user_id}/gyroscope_advanced/"))

                # Invia a Kafka
                try:
                    send_to_kafka(user_data, "gyroscope_advanced")
                    print(f"📤 Gyroscope data sent to Kafka for user {user_id}")
                except Exception as e:
                    print(f"⚠️ Error sending gyroscope data to Kafka for user {user_id}: {e}")

        print(f"🧭 Saved and sent gyroscope metrics for {len(user_ids)} users")

    # Altimeter Metrics
    if sensors['altitude'].take(1):
        altitude_advanced = (
            sensors['altitude']
            .withColumn("altitude_metrics", advanced_altitude_metrics(col("raw_values")))
            .select(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("altitude_metrics.cumulative_ascent").alias("cumulative_ascent_m"),
                col("altitude_metrics.cumulative_descent").alias("cumulative_descent_m"),
                col("altitude_metrics.avg_altitude").alias("avg_altitude_m"),
                col("altitude_metrics.avg_slope").alias("avg_slope_percent")
            )
        )

        user_ids = [row.user_id for row in altitude_advanced.select("user_id").distinct().collect()]
        for user_id in user_ids:
            user_data = altitude_advanced.filter(col("user_id") == user_id)
            if user_data.take(1):
                # Salva su MinIO
                (user_data.coalesce(1).write.format("delta").mode("append")
                 .option("mergeSchema", "true")
                 .save(f"s3a://gold/users/{user_id}/altimeter_advanced/"))

                # Invia a Kafka
                try:
                    send_to_kafka(user_data, "altimeter_advanced")
                    print(f"📤 Altimeter data sent to Kafka for user {user_id}")
                except Exception as e:
                    print(f"⚠️ Error sending altimeter data to Kafka for user {user_id}: {e}")

        print(f"⛰️ Saved and sent altimeter metrics for {len(user_ids)} users")

    # Barometer Metrics
    if sensors['pressure'].take(1):
        pressure_advanced = (
            sensors['pressure']
            .withColumn("pressure_metrics", advanced_pressure_metrics(col("raw_values")))
            .select(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("pressure_metrics.pressure_trend").alias("pressure_trend"),
                col("pressure_metrics.estimated_altitude").alias("barometric_altitude_m"),
                col("avg_value").alias("avg_pressure_hpa")
            )
        )

        user_ids = [row.user_id for row in pressure_advanced.select("user_id").distinct().collect()]
        for user_id in user_ids:
            user_data = pressure_advanced.filter(col("user_id") == user_id)
            if user_data.take(1):
                # Salva su MinIO
                (user_data.coalesce(1).write.format("delta").mode("append")
                 .option("mergeSchema", "true")
                 .save(f"s3a://gold/users/{user_id}/barometer_advanced/"))

                # Invia a Kafka
                try:
                    send_to_kafka(user_data, "barometer_advanced")
                    print(f"📤 Barometer data sent to Kafka for user {user_id}")
                except Exception as e:
                    print(f"⚠️ Error sending barometer data to Kafka for user {user_id}: {e}")

        print(f"🌊 Saved and sent barometer metrics for {len(user_ids)} users")

    # CEDA Metrics (con dati accelerometro se disponibili)
    if sensors['ceda'].take(1):
        # Prova a fare join con accelerometro per migliorare METs
        ceda_base = sensors['ceda']
        accel_for_ceda = None

        if sensors['accel'].take(1):
            accel_for_ceda = (
                sensors['accel']
                .select("user_id", "window_start", "raw_values")
                .withColumnRenamed("raw_values", "accel_values")
            )

            ceda_enhanced = (
                ceda_base.alias("c")
                .join(
                    accel_for_ceda.alias("a"),
                    (col("c.user_id") == col("a.user_id")) &
                    (col("c.window_start") == col("a.window_start")),
                    "left"
                )
                .select("c.*", "a.accel_values")
            )
        else:
            ceda_enhanced = ceda_base.withColumn("accel_values", lit(None))

        ceda_advanced = (
            ceda_enhanced
            .withColumn("ceda_metrics", advanced_ceda_metrics(col("raw_values"), col("accel_values")))
            .select(
                col("user_id"),
                col("window_start"),
                col("window_end"),
                col("ceda_metrics.energy_expenditure_cal").alias("energy_expenditure_cal_per_min"),
                col("ceda_metrics.mets").alias("metabolic_equivalents"),
                col("ceda_metrics.activity_class").alias("activity_classification"),
                col("avg_value").alias("avg_eda_microsiemens")
            )
        )

        user_ids = [row.user_id for row in ceda_advanced.select("user_id").distinct().collect()]
        for user_id in user_ids:
            user_data = ceda_advanced.filter(col("user_id") == user_id)
            if user_data.take(1):
                # Salva su MinIO
                (user_data.coalesce(1).write.format("delta").mode("append")
                 .option("mergeSchema", "true")
                 .save(f"s3a://gold/users/{user_id}/ceda_advanced/"))

                # Invia a Kafka
                try:
                    send_to_kafka(user_data, "ceda_advanced")
                    print(f"📤 CEDA data sent to Kafka for user {user_id}")
                except Exception as e:
                    print(f"⚠️ Error sending CEDA data to Kafka for user {user_id}: {e}")

        print(f"⚡ Saved and sent CEDA metrics for {len(user_ids)} users")

    print(f"✅ Batch {batch_id} - Advanced gold Layer metrics calculated, saved per user, and sent to Kafka")


# =========================================
# Adaptive Trigger System
# =========================================
import time
import os

# Variabile globale per tracciare se è il primo run
checkpoint_path = "s3a://gold/checkpoints/advanced_metrics_per_user/"
first_run_marker = "s3a://gold/checkpoints/first_run_complete.marker"


# Funzione per verificare se è il primo run
def is_first_run():
    try:
        # Prova a leggere il marker file
        marker_df = spark.read.text(first_run_marker)
        return False  # Se esiste, non è il primo run
    except:
        return True  # Se non esiste, è il primo run


# Funzione wrapper per gestire il timing
def adaptive_batch_processor(batch_df, batch_id):
    start_time = time.time()

    if is_first_run() and batch_id == 0:
        print(f"🚀 COLD START - Batch {batch_id} (using 1 minute window)")
        # Per il primo batch, usa la logica normale
        calculate_advanced_gold_metrics(batch_df, batch_id)

        # Crea marker file per indicare che il primo run è completato
        try:
            marker_data = spark.createDataFrame([("first_run_completed_at", str(time.time()))], ["status", "timestamp"])
            marker_data.write.mode("overwrite").text(first_run_marker)
            print("✅ First run marker created")
        except Exception as e:
            print(f"⚠️ Could not create first run marker: {e}")
    else:
        print(f"⚡ FAST PROCESSING - Batch {batch_id} (using 5 second intervals)")
        # Per i batch successivi, usa finestre più piccole ma più frequenti
        calculate_fast_metrics(batch_df, batch_id)

    processing_time = time.time() - start_time
    print(f"⏱️ Batch {batch_id} processed in {processing_time:.2f} seconds")


# Funzione ottimizzata per elaborazioni veloci
def calculate_fast_metrics(batch_df, batch_id):
    """Versione ottimizzata per elaborazioni rapide successive"""
    if not batch_df.take(1):
        print(f"⚠️ Batch {batch_id} vuoto")
        return

    print(f"🔄 Fast processing batch {batch_id}...")

    # Usa finestre più piccole per aggiornamenti rapidi (15 secondi invece di 1 minuto)
    windowed_df = (
        batch_df
        .withWatermark("datetime", "30 seconds")  # Watermark ridotto
        .groupBy(
            col("user_id"),
            window(col("datetime"), "15 seconds"),  # Finestra ridotta
            col("measurement_type")
        )
        .agg(
            count("*").alias("sample_count"),
            avg("value").alias("avg_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value"),
            stddev("value").alias("std_value"),
            collect_list("value").alias("raw_values"),
            collect_list("timestamp").alias("timestamps"),
            first("timestamp").alias("window_start_ts"),
            last("timestamp").alias("window_end_ts")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .filter(col("sample_count") >= 5)  # Soglia ridotta per finestre più piccole
    )

    # Elabora solo i sensori che hanno dati nel batch corrente
    active_sensors = [row.measurement_type for row in windowed_df.select("measurement_type").distinct().collect()]

    # Elaborazione rapida per sensori attivi
    for sensor_type in active_sensors:
        sensor_data = windowed_df.filter(col("measurement_type") == sensor_type)

        if sensor_type == "ppg" and sensor_data.take(1):
            # PPG veloce - solo HR base
            ppg_fast = (
                sensor_data
                .withColumn("hr_estimate",
                            when(col("sample_count") >= 10,
                                 expr("60.0 / ((max_value - min_value) * 0.02)")  # Stima veloce
                                 ).otherwise(None)
                            )
                .select("user_id", "window_start", "window_end", "hr_estimate", "avg_value")
            )

            # Salva per ogni user e invia a Kafka
            user_ids = [row.user_id for row in ppg_fast.select("user_id").distinct().collect()]
            for user_id in user_ids:
                user_data = ppg_fast.filter(col("user_id") == user_id)
                if user_data.take(1):
                    # Salva su MinIO
                    (user_data.coalesce(1).write.format("delta").mode("append")
                     .option("mergeSchema", "true")
                     .save(f"s3a://gold/users/{user_id}/ppg_realtime/"))

                    # Invia a Kafka
                    try:
                        send_to_kafka(user_data, "ppg_realtime")
                        print(f"📤 Fast PPG data sent to Kafka for user {user_id}")
                    except Exception as e:
                        print(f"⚠️ Error sending fast PPG data to Kafka: {e}")

        elif sensor_type == "accelerometer" and sensor_data.take(1):
            # Accelerometro veloce - solo step detection base
            accel_fast = (
                sensor_data
                .withColumn("movement_level",
                            when(col("std_value") > 2.0, "active")
                            .when(col("std_value") > 0.5, "light")
                            .otherwise("sedentary")
                            )
                .withColumn("step_estimate",
                            when(col("std_value") > 1.0, expr("std_value * 2")).otherwise(0)
                            )
                .select("user_id", "window_start", "window_end", "movement_level", "step_estimate")
            )

            user_ids = [row.user_id for row in accel_fast.select("user_id").distinct().collect()]
            for user_id in user_ids:
                user_data = accel_fast.filter(col("user_id") == user_id)
                if user_data.take(1):
                    # Salva su MinIO
                    (user_data.coalesce(1).write.format("delta").mode("append")
                     .option("mergeSchema", "true")
                     .save(f"s3a://gold/users/{user_id}/movement_realtime/"))

                    # Invia a Kafka
                    try:
                        send_to_kafka(user_data, "movement_realtime")
                        print(f"📤 Fast movement data sent to Kafka for user {user_id}")
                    except Exception as e:
                        print(f"⚠️ Error sending fast movement data to Kafka: {e}")

        elif sensor_type == "skin_temp" and sensor_data.take(1):
            # Temperatura veloce
            temp_fast = (
                sensor_data
                .select("user_id", "window_start", "window_end",
                        col("avg_value").alias("temp_avg"),
                        col("std_value").alias("temp_variability"))
            )

            user_ids = [row.user_id for row in temp_fast.select("user_id").distinct().collect()]
            for user_id in user_ids:
                user_data = temp_fast.filter(col("user_id") == user_id)
                if user_data.take(1):
                    # Salva su MinIO
                    (user_data.coalesce(1).write.format("delta").mode("append")
                     .option("mergeSchema", "true")
                     .save(f"s3a://gold/users/{user_id}/temperature_realtime/"))

                    # Invia a Kafka
                    try:
                        send_to_kafka(user_data, "temperature_realtime")
                        print(f"📤 Fast temperature data sent to Kafka for user {user_id}")
                    except Exception as e:
                        print(f"⚠️ Error sending fast temperature data to Kafka: {e}")

    print(f"⚡ Fast batch {batch_id} completed - {len(active_sensors)} sensors processed and sent to Kafka")


# =========================================
# Funzione per creare summary giornalieri e inviarli a Kafka
# =========================================
def create_daily_summary_for_users():
    """Crea summary giornalieri combinando tutte le metriche per user e li invia a Kafka"""
    print("📈 Creating daily summaries for users...")

    # Leggi tutte le metriche salvate per creare summary
    try:
        # Questo potrebbe essere eseguito come job separato o schedulato
        users_base_path = "s3a://gold/users/"

        # Per ogni utente, combina le metriche giornaliere
        # Questo è un esempio di come potrebbe funzionare
        summary_query = """
                        SELECT user_id,
                               DATE(window_start)                  as date,

                               -- PPG Summary 
                               AVG(heart_rate_bpm)                 as avg_hr_daily,
                               AVG(hrv_sdnn_ms)                    as avg_hrv_sdnn_daily,
                               AVG(stress_index)                   as avg_stress_daily,
                               AVG(spo2_percent)                   as avg_spo2_daily,

                               -- Steps Summary   
                               SUM(step_count)                     as total_steps_daily,
                               AVG(cadence_spm)                    as avg_cadence_daily,
                               SUM(active_time_seconds) / 60       as total_active_minutes,
                               SUM(sedentary_time_seconds) / 60    as total_sedentary_minutes,

                               -- Energy Summary 
                               SUM(energy_expenditure_cal_per_min) as total_calories_daily,
                               AVG(metabolic_equivalents)          as avg_mets_daily,

                               -- Altitude Summary 
                               SUM(cumulative_ascent_m)            as total_ascent_daily,
                               SUM(cumulative_descent_m)           as total_descent_daily,

                               -- Temperature Summary 
                               AVG(avg_temp_celsius)               as avg_body_temp_daily,
                               SUM(rapid_temp_changes)             as total_temp_anomalies,

                               -- Activity Summary 
                               MODE(activity_classification)       as dominant_activity_daily,
                               COUNT(*)                            as total_measurements,

                               -- Wellness Score 
                               CASE
                                   WHEN AVG(heart_rate_bpm) BETWEEN 60 AND 100
                                       AND SUM(step_count) > 8000
                                       AND SUM(active_time_seconds) / 60 > 30
                                       AND AVG(stress_index) < 50 THEN 'Excellent'
                                   WHEN SUM(step_count) > 5000
                                       AND SUM(active_time_seconds) / 60 > 20 THEN 'Good'
                                   WHEN SUM(step_count) > 2000 THEN 'Fair'
                                   ELSE 'Poor'
                                   END                             as daily_wellness_score

                        FROM combined_user_metrics
                        GROUP BY user_id, DATE(window_start) \
                        """

        print("📊 Daily summary structure defined")

    except Exception as e:
        print(f"⚠️ Error creating daily summary: {e}")


# =========================================
# Streaming per aggregazioni real-time con Kafka
# =========================================
def create_realtime_dashboard_data(batch_df, batch_id):
    """Crea dati per dashboard real-time e li invia a Kafka"""
    if not batch_df.take(1):
        return

    # Crea metriche aggregate per dashboard
    current_stats = (
        batch_df
        .groupBy("user_id")
        .agg(
            count("*").alias("total_sensors_active"),
            countDistinct("measurement_type").alias("sensor_types_count"),
            avg("latency_sec").alias("avg_latency"),
            current_timestamp().alias("last_update")
        )
    )

    # Salva stats per dashboard su MinIO
    if current_stats.take(1):
        (current_stats.coalesce(1).write.format("delta").mode("overwrite")
         .save("s3a://gold/dashboard/realtime_stats/"))

        # Invia anche a Kafka per dashboard real-time
        try:
            send_to_kafka(current_stats, "dashboard_stats")
            print(f"📤 Dashboard stats sent to Kafka for batch {batch_id}")
        except Exception as e:
            print(f"⚠️ Error sending dashboard stats to Kafka: {e}")


# =========================================
# Streaming Query con Trigger Adattivo
# =========================================
if is_first_run():
    print("🚀 Starting with COLD START mode (1 minute trigger)")
    trigger_interval = "1 minute"
else:
    print("⚡ Starting with FAST mode (5 second trigger)")
    trigger_interval = "5 seconds"

query = (
    df_silver.writeStream
    .foreachBatch(adaptive_batch_processor)
    .outputMode("append")
    .trigger(processingTime=trigger_interval)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

# Query per dashboard real-time
dashboard_query = (
    df_silver.writeStream
    .foreachBatch(create_realtime_dashboard_data)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "s3a://gold/checkpoints/dashboard_stats/")
    .start()
)

print("✅ Advanced gold Layer per-user streaming avviato")
print("📁 Output structure per user:")
print("   MinIO: s3a://gold/users/{user_id}/ppg_advanced/")
print("   MinIO: s3a://gold/users/{user_id}/temperature_advanced/")
print("   MinIO: s3a://gold/users/{user_id}/accelerometer_advanced/")
print("   MinIO: s3a://gold/users/{user_id}/gyroscope_advanced/")
print("   MinIO: s3a://gold/users/{user_id}/altimeter_advanced/")
print("   MinIO: s3a://gold/users/{user_id}/barometer_advanced/")
print("   MinIO: s3a://gold/users/{user_id}/ceda_advanced/")
print("")
print("📤 Kafka Output:")
print("   Topic: gold_layer")
print("   Message format: JSON with key=user_id, value=metrics")
print("   Sensor types: ppg_advanced, temperature_advanced, accelerometer_advanced,")
print("                gyroscope_advanced, altimeter_advanced, barometer_advanced, ceda_advanced")
print("")
print("🔬 Advanced Metrics Calculated:")
print("💓 PPG: HR, HRV (SDNN, RMSSD), Stress Index, SpO2")
print("🌡️ Skin Temp: Avg Temp, Temp Trend, Rapid Change Detection")
print("🚶 Accelerometer: Step Count, Cadence, VM, Sedentary/Active Time")
print("🧭 Gyroscope: Angular Velocity, Posture Change, Fall Detection")
print("⛰️ Altimeter: Cum. Ascent/Descent, Avg Altitude, Slope")
print("🌊 Barometer: Pressure Trend, Estimated Altitude")
print("⚡ CEDA: Energy Expenditure, METs, Activity Classification")
print("")
print("⏱️ Processing window: 1 minute per user (cold start), 15 seconds (fast mode)")
print("📊 Trigger interval: 1 minute (cold start), 5 seconds (fast mode)")
print("📊 Real-time dashboard data stream started")


# =========================================
# Stream diretto a Kafka per metriche specifiche (opzionale)
# =========================================
def create_direct_kafka_streams():
    """Crea stream diretti a Kafka per metriche specifiche senza batch processing"""

    # Stream PPG diretto
    ppg_stream = (
        df_silver
        .filter(col("measurement_type") == "ppg")
        .withWatermark("datetime", "1 minute")
        .groupBy(
            col("user_id"),
            window(col("datetime"), "30 seconds"),
        )
        .agg(
            avg("value").alias("avg_ppg"),
            count("*").alias("sample_count")
        )
        .withColumn("estimated_hr",
                    when(col("sample_count") >= 15,
                         expr("60.0 / (avg_ppg * 0.01)")).otherwise(None))
        .select(
            col("user_id").cast("string").alias("key"),
            to_json(struct(
                col("user_id"),
                col("window.start").alias("timestamp"),
                col("estimated_hr"),
                col("avg_ppg"),
                lit("ppg_stream").alias("stream_type")
            )).alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker_kafka:9092")
        .option("topic", "gold_layer")
        .option("checkpointLocation", "s3a://gold/checkpoints/kafka_ppg_stream/")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("🌊 Direct PPG stream to Kafka started")
    return ppg_stream


# Avvia stream diretti (opzionale)
# direct_ppg_stream = create_direct_kafka_streams()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("⏹️ Arresto Advanced gold Layer richiesto dall'utente")
    query.stop()
    dashboard_query.stop()
    # if 'direct_ppg_stream' in locals():
    #     direct_ppg_stream.stop()
finally:
    spark.stop()