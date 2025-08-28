# spark_scripts/train_classifiers.py

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# --- CONFIGURAZIONE GLOBALE ---
CONFIG = {
    "nutrition": {
        "training_data_path": "s3a://gold/nutrition/training_data/",
        "recommendations_path": "s3a://gold/nutrition/recommendations/",
        "user_cluster_map_path": "s3a://gold/models/nutrition/user_cluster_map/",
        "model_output_path": "s3a://gold/models/nutrition/classification_models/",
        # Feature utente
        "user_features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
            "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
        ],
        # Feature raccomandazione
        "rec_features": ["recommendation_type_indexed"]
    },
    "workout": {
        "training_data_path": "s3a://gold/workout/training_data/",
        "recommendations_path": "s3a://gold/workout/recommendations/",
        "user_cluster_map_path": "s3a://gold/models/workout/user_cluster_map/",
        "model_output_path": "s3a://gold/models/workout/classification_models/",
        # Feature utente
        "user_features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
        ],
        # Feature raccomandazione
        "rec_features": ["type_indexed", "difficulty_indexed", "duration"]
    }
}
K_CLUSTERS = 4
MIN_ROWS_PER_CLUSTER = 20  # soglia minima per addestrare


# ---------- Spark & S3A ----------
def get_spark_session():
    builder = SparkSession.builder.appName("train_classifiers")

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


# ---------- Helpers ----------
def normalize_schema(training_df, rec_df):
    """
    - Uniforma l'id raccomandazione a 'id_recommendation'.
    - Crea 'label' da:
        * feedback == 'positive'
        * is_positive True/1/'true'/'1'
    """
    if "id_recommendation" not in training_df.columns and "recommendation_id" in training_df.columns:
        training_df = training_df.withColumnRenamed("recommendation_id", "id_recommendation")

    if "id_recommendation" not in rec_df.columns and "recommendation_id" in rec_df.columns:
        rec_df = rec_df.withColumnRenamed("recommendation_id", "id_recommendation")

    cols = set(training_df.columns)
    if "feedback" in cols:
        training_df = training_df.withColumn("label", when(col("feedback") == "positive", 1).otherwise(0))
    elif "is_positive" in cols:
        training_df = training_df.withColumn(
            "label",
            when((col("is_positive") == True) | (col("is_positive") == 1) |
                 (col("is_positive") == "true") | (col("is_positive") == "1"), 1).otherwise(0)
        )
    else:
        raise ValueError("Mancano sia 'feedback' sia 'is_positive' nel training data.")

    return training_df, rec_df


# ---------- Training ----------
def train_for_domain(spark: SparkSession, domain: str):
    print("\n===================================================")
    print(f"🚀 AVVIO TRAINING PIPELINE: {domain.upper()}")
    print("===================================================")

    cfg = CONFIG[domain]

    # 1) Load
    print(f"[{domain}] 📦 Carico training data → {cfg['training_data_path']}")
    training_data_df = spark.read.parquet(cfg['training_data_path'])
    if training_data_df.rdd.isEmpty():
        print(f"[{domain}] ⚠️ Training vuoto. Skip dominio.")
        return

    print(f"[{domain}] 📦 Carico user_cluster_map → {cfg['user_cluster_map_path']}")
    user_cluster_map_df = spark.read.parquet(cfg['user_cluster_map_path'])
    if user_cluster_map_df.rdd.isEmpty():
        print(f"[{domain}] ⚠️ user_cluster_map vuoto. Skip dominio.")
        return

    print(f"[{domain}] 📦 Carico catalogo raccomandazioni → {cfg['recommendations_path']}")
    rec_df = spark.read.parquet(cfg['recommendations_path'])
    if rec_df.rdd.isEmpty():
        print(f"[{domain}] ⚠️ Catalogo raccomandazioni vuoto. Skip dominio.")
        return

    # 2) Normalize schema + join
    training_data_df, rec_df = normalize_schema(training_data_df, rec_df)
    master_df = (training_data_df
                 .join(user_cluster_map_df, "user_id")
                 .join(rec_df, "id_recommendation"))

    # Normalizza colonne per workout
    if domain == "workout":
        if "type" not in master_df.columns and "category" in master_df.columns:
            master_df = master_df.withColumn("type", col("category"))
            print("[workout] Mappo 'category' -> 'type'")
        if "duration" not in master_df.columns and "duration_minutes" in master_df.columns:
            master_df = master_df.withColumn("duration", col("duration_minutes"))
            print("[workout] Mappo 'duration_minutes' -> 'duration'")

    # --- DEBUG: ispezione schema e sample prima del training ---
    print(f"[{domain}] 🧪 master_df.printSchema():")
    master_df.printSchema()

    print(f"[{domain}] 🧾 master_df.sample (5 righe):")
    master_df.select(
        "user_id", "cluster_id", "id_recommendation",
        "gender", "age", "height", "avg_weight_last7d", "bmi",
        # workout fields (se presenti)
        *[c for c in ["avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d", "type", "difficulty", "duration"] if c in master_df.columns],
        # nutrition fields (se presenti)
        *[c for c in ["calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
                      "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg", "recommendation_type"] if c in master_df.columns],
        "label"
    ).show(5, truncate=False)

    # 3) Indexers
    indexers = [StringIndexer(inputCol="gender", outputCol="gender_indexed", handleInvalid="skip")]
    if domain == "nutrition":
        if "recommendation_type" in master_df.columns:
            indexers.append(StringIndexer(inputCol="recommendation_type",
                                          outputCol="recommendation_type_indexed",
                                          handleInvalid="skip"))
    else:  # workout
        if "type" in master_df.columns:
            indexers.append(StringIndexer(inputCol="type", outputCol="type_indexed", handleInvalid="skip"))
        if "difficulty" in master_df.columns:
            indexers.append(StringIndexer(inputCol="difficulty", outputCol="difficulty_indexed", handleInvalid="skip"))

    # 4) Feature list
    feature_cols = cfg['user_features'] + cfg['rec_features']
    missing = [c for c in feature_cols if c not in master_df.columns]
    if missing:
        print(f"[{domain}] ⚠️ Mancano queste feature, le escludo: {missing}")
    feature_cols = [c for c in feature_cols if c in master_df.columns]
    print(f"[{domain}] 🔧 Feature usate: {feature_cols}")

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
    pipeline = Pipeline(stages=indexers + [assembler, rf])

    # 5) Training per cluster
    for i in range(K_CLUSTERS):
        print(f"\n[{domain}] === Cluster {i} ===")
        cluster_df = master_df.filter(col("cluster_id") == i)

        n_rows = cluster_df.count()
        print(f"[{domain}]   • Righe cluster {i}: {n_rows}")
        if n_rows < MIN_ROWS_PER_CLUSTER:
            print(f"[{domain}]   ⚠️ Dati insufficienti (<{MIN_ROWS_PER_CLUSTER}). Skip cluster {i}.")
            continue

        train_df, test_df = cluster_df.randomSplit([0.8, 0.2], seed=42)
        n_tr, n_te = train_df.count(), test_df.count()
        print(f"[{domain}]   • Split: train={n_tr}, test={n_te}")

        print(f"[{domain}]   🤖 Addestro RandomForest...")
        model = pipeline.fit(train_df)

        if n_te > 0:
            preds = model.transform(test_df)
            evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                                      metricName="areaUnderROC")
            auc = evaluator.evaluate(preds)
            print(f"[{domain}]   ✅ AUC cluster {i}: {auc:.4f}")
        else:
            print(f"[{domain}]   ℹ️ Nessun test set. Skipping AUC.")

        print(f"[{domain}]   ♻️ Refit su tutto il cluster e salvo modello...")
        final_model = pipeline.fit(cluster_df)
        model_path = f"{cfg['model_output_path']}cluster_{i}"
        print(f"[{domain}]   💾 Salvataggio: {model_path}")
        final_model.write().overwrite().save(model_path)

    print(f"\n[{domain}] 🎉 TRAINING COMPLETATO.")


# ---------- Main ----------
if __name__ == "__main__":
    print("\n=== START JOB: TRAIN CLASSIFIERS (WORKOUT → NUTRITION) ===")
    spark = get_spark_session()
    try:
        train_for_domain(spark, "workout")
        train_for_domain(spark, "nutrition")
        print("\n✅ DONE: workout ✓  |  nutrition ✓")
    finally:
        spark.stop()
        print("🧹 SparkSession chiusa.")
