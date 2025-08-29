# spark_scripts/run_clustering.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.clustering import KMeans

# =========================
# CONFIG
# =========================
CONFIG = {
    "nutrition": {
        "training_data_path": "s3a://gold/nutrition/training_data/",
        "model_output_path": "s3a://gold/models/nutrition/clustering_model/",
        "map_output_path": "s3a://gold/models/nutrition/user_cluster_map/",
        "features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
            "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
        ]
    },
    "workout": {
        "training_data_path": "s3a://gold/workout/training_data/",
        "model_output_path": "s3a://gold/models/workout/clustering_model/",
        "map_output_path": "s3a://gold/models/workout/user_cluster_map/",
        "features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
        ]
    }
}

K_CLUSTERS = 4


# =========================
# Spark Session
# =========================
def get_spark_session():
    builder = SparkSession.builder.appName("clustering")

    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_path_style = os.getenv("S3_PATH_STYLE", "true")
    s3_ssl = os.getenv("S3_SSL_ENABLED", "false")

    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    minio_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    builder = (
        builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", s3_path_style)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_ssl)
        .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("S3_REGION", "us-east-1"))
    )

    if aws_key and aws_secret:
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider"
        )
    else:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", minio_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        )

    return builder.getOrCreate()


# =========================
# Utils
# =========================
def safe_drop(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.drop(c)
    return df


# =========================
# Clustering per dominio
# =========================
def run_clustering_for_domain(spark: SparkSession, domain: str):
    print(f"\n==============================")
    print(f"🚀 AVVIO CLUSTERING: {domain.upper()}")
    print(f"==============================")

    cfg = CONFIG[domain]
    print(f"[{domain}] 📦 Lettura dati: {cfg['training_data_path']}")
    full_df = spark.read.parquet(cfg['training_data_path'])

    # Assicuro 'date' come date (serve per ordinare l'ultima riga per utente)
    if "date" not in full_df.columns:
        raise ValueError(f"[{domain}] La colonna 'date' non è presente nel dataset.")
    full_df = full_df.withColumn("date", col("date").cast("date"))

    print(f"[{domain}] 🧪 Schema completo sorgente:")
    full_df.printSchema()

    # 🚫 NESSUN FILTRO TEMPORALE: uso tutte le righe disponibili
    training_df = full_df

    print(f"[{domain}] 📊 Sample righe (tutte le date):")
    training_df.show(5, truncate=False)

    # Ultimo stato per utente (righe più recenti per ogni user_id)
    print(f"[{domain}] 🧮 Calcolo ultimo stato per utente...")
    window_spec = Window.partitionBy("user_id").orderBy(col("date").desc())
    user_features_df = (
        training_df
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .drop("rank")
    )

    # Drop di colonne non utili al modello (se presenti)
    user_features_df = safe_drop(
        user_features_df,
        ["date", "recommendation_id", "is_positive", "noted_at", "id_cluster"]
    )

    print(f"[{domain}] 🧾 Prime righe delle feature (pre-pipeline):")
    user_features_df.show(5, truncate=False)

    # =========================
    # PIPELINE ML (pulita)
    # =========================
    required = list(cfg["features"])
    needs_gender_indexed = "gender_indexed" in required

    # validazione leggera
    missing = [c for c in required if c != "gender_indexed" and c not in user_features_df.columns]
    if needs_gender_indexed and "gender" not in user_features_df.columns:
        raise ValueError(f"[{domain}] 'gender_indexed' richiede la colonna 'gender' che non è presente.")
    if missing:
        raise ValueError(f"[{domain}] Mancano colonne feature: {missing}")

    stages = []
    if needs_gender_indexed:
        stages.append(StringIndexer(
            inputCol="gender",
            outputCol="gender_indexed",
            handleInvalid="skip"
        ))

    assembler_inputs = required[:] if needs_gender_indexed else [c for c in required if c != "gender_indexed"]

    assembler = VectorAssembler(
        inputCols=assembler_inputs,
        outputCol="unscaled_features",
        handleInvalid="skip"
    )
    scaler = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features")
    kmeans = KMeans(featuresCol="scaled_features", k=K_CLUSTERS, seed=42, predictionCol="cluster_id")

    pipeline = Pipeline(stages=stages + [assembler, scaler, kmeans])

    print(f"[{domain}] 🤖 Training KMeans (k={K_CLUSTERS})...")
    model = pipeline.fit(user_features_df)

    # Salva modello e mappa utente→cluster
    print(f"[{domain}] 💾 Salvataggio modello → {cfg['model_output_path']}")
    model.write().overwrite().save(cfg['model_output_path'])

    print(f"[{domain}] 🗺️ Generazione mappa utente→cluster → {cfg['map_output_path']}")
    user_cluster_map = model.transform(user_features_df).select("user_id", "cluster_id")
    user_cluster_map.write.mode("overwrite").parquet(cfg['map_output_path'])

    print(f"[{domain}] ✅ Clustering COMPLETATO.")


# =========================
# Main
# =========================
if __name__ == "__main__":
    print("\n=== START JOB: WORKOUT poi NUTRITION ===")
    spark = get_spark_session()
    try:
        run_clustering_for_domain(spark, "workout")
        run_clustering_for_domain(spark, "nutrition")
        print("\n🎉 TUTTO COMPLETATO: workout ✓  |  nutrition ✓")
    finally:
        spark.stop()
        print("🧹 SparkSession chiusa.")
