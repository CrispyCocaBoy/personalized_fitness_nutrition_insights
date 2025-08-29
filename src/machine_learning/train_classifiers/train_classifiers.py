# spark_scripts/train_classifiers.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
        "user_features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
            "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
        ],
        "rec_features": ["recommendation_type_indexed"]
    },
    "workout": {
        "training_data_path": "s3a://gold/workout/training_data/",
        "recommendations_path": "s3a://gold/workout/recommendations/",
        "user_cluster_map_path": "s3a://gold/models/workout/user_cluster_map/",
        "model_output_path": "s3a://gold/models/workout/classification_models/",
        "user_features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
        ],
        "rec_features": ["type_indexed", "difficulty_indexed", "duration"]
    }
}
K_CLUSTERS = 4

def get_spark_session():
    builder = SparkSession.builder.appName("TrainingPipeline")

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

def train_for_domain(spark, domain: str):
    print(f"\n--- AVVIO TRAINING PIPELINE: {domain.upper()} ---")

    cfg = CONFIG[domain]

    # 1) Caricamento dati
    print("Caricamento dati...")
    training_data_df = spark.read.parquet(cfg['training_data_path'])
    user_cluster_map_df = spark.read.parquet(cfg['user_cluster_map_path'])
    recommendations_df = spark.read.parquet(cfg['recommendations_path'])

    # 2) Master DF e label
    master_df = (
        training_data_df
        .join(user_cluster_map_df, "user_id")
        .join(recommendations_df, "recommendation_id")
        .withColumn("label", col("is_positive").cast("integer"))
    )

    # 3) Loop per cluster
    for i in range(K_CLUSTERS):
        print(f"\n--- Addestramento Modello per Cluster {i} ---")
        cluster_df = master_df.filter(col("cluster_id") == i)

        n_rows = cluster_df.count()
        if n_rows < 20:
            print(f"Cluster {i} non ha dati di training a sufficienza (rows={n_rows}). Salto.")
            continue

        # 4) Split
        train_df, test_df = cluster_df.randomSplit([0.8, 0.2], seed=42)
        print(f"Dimensioni dataset: Training={train_df.count()}, Test={test_df.count()}")

        # 5) Pipeline ML
        indexers = []
        if "gender" in cluster_df.columns:
            indexers.append(StringIndexer(inputCol="gender", outputCol="gender_indexed", handleInvalid="skip"))

        if domain == "nutrition":
            if "recommendation_type" in cluster_df.columns:
                indexers.append(StringIndexer(inputCol="recommendation_type",
                                              outputCol="recommendation_type_indexed",
                                              handleInvalid="skip"))
        else:  # workout
            if "type" in cluster_df.columns:
                indexers.append(StringIndexer(inputCol="type", outputCol="type_indexed", handleInvalid="skip"))
            if "difficulty" in cluster_df.columns:
                indexers.append(StringIndexer(inputCol="difficulty", outputCol="difficulty_indexed", handleInvalid="skip"))

        # Assembler
        desired_features = cfg['user_features'] + cfg['rec_features']
        feature_cols = [c for c in desired_features if c in cluster_df.columns]
        if not feature_cols:
            print(f"Cluster {i}: nessuna feature disponibile tra {desired_features}. Salto.")
            continue

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
        pipeline = Pipeline(stages=indexers + [assembler, rf])

        # 6) Train + Eval
        print("Addestramento del modello sul training set...")
        model = pipeline.fit(train_df)

        print("Valutazione del modello sul test set...")
        predictions = model.transform(test_df)
        evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                                  metricName="areaUnderROC")
        try:
            auc = evaluator.evaluate(predictions)
            print(f"===> Performance Cluster {i} - Test AUC: {auc:.4f} <===")
        except Exception as e:
            print(f"AUC non calcolabile (test monoclasse o vuoto): {e}")

        # 7) Refit e salvataggio
        print("Ri-addestramento del modello su tutti i dati del cluster e salvataggio...")
        final_model = pipeline.fit(cluster_df)
        model_path = f"{cfg['model_output_path']}cluster_{i}"
        print(f"Salvataggio modello finale in: {model_path}")
        final_model.write().overwrite().save(model_path)

    print(f"\n--- TRAINING PIPELINE {domain.upper()} COMPLETATA ---")

if __name__ == "__main__":
    spark_session = get_spark_session()
    try:
        train_for_domain(spark_session, "workout")
        train_for_domain(spark_session, "nutrition")
        print("\nDONE: workout | nutrition")
    finally:
        spark_session.stop()
        print("SparkSession chiusa.")
