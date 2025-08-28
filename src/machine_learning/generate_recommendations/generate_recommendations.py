# spark_scripts/generate_recommendations.py

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, row_number, collect_set, explode,
    element_at
)
from pyspark.ml.functions import vector_to_array
from pyspark.sql.window import Window
from pyspark.ml.pipeline import PipelineModel

# --- CONFIGURAZIONE GLOBALE ---
CONFIG = {
    "nutrition": {
        "user_cluster_map_path": "s3a://gold/models/nutrition/user_cluster_map/",
        "recommendations_path": "s3a://gold/nutrition/recommendations/",     # catalogo raccomandazioni
        "classification_models_path": "s3a://gold/models/nutrition/classification_models/",
        "historical_data_path": "s3a://gold/nutrition/training_data/",
        "db_table": "user_nutrition_rankings"
    },
    "workout": {
        "user_cluster_map_path": "s3a://gold/models/workout/user_cluster_map/",
        "recommendations_path": "s3a://gold/workout/recommendations/",
        "classification_models_path": "s3a://gold/models/workout/classification_models/",
        "historical_data_path": "s3a://gold/workout/training_data/",
        "db_table": "user_workout_rankings"
    }
}

K_CLUSTERS = 4
TOP_N_RECS = 10  # quante raccomandazioni per utente

# ---------- Spark & S3A ----------
def get_spark_session():
    builder = SparkSession.builder.appName("recommendation")
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
def normalize_schema_for_history(df):
    """
    Normalizza le colonne chiave nel dataset storico:
    - recommendation_id -> id_recommendation (se necessario)
    - ricava una colonna 'feedback_positive' booleana da:
        * feedback == 'positive'  (se esiste)
        * is_positive in {True, 1, 'true', '1'}  (fallback)
    """
    cols = set(df.columns)

    # Uniforma recommendation id
    if "id_recommendation" not in cols and "recommendation_id" in cols:
        df = df.withColumnRenamed("recommendation_id", "id_recommendation")
        cols.add("id_recommendation")

    # Crea flag feedback_positive
    if "feedback" in cols:
        df = df.withColumn("feedback_positive", col("feedback") == "positive")
    elif "is_positive" in cols:
        # gestiamo True/False, 1/0, 'true'/'false'
        df = df.withColumn(
            "feedback_positive",
            (col("is_positive") == True) | (col("is_positive") == 1) | (col("is_positive") == "true") | (col("is_positive") == "1")
        )
    else:
        raise ValueError("Nel dataset storico mancano sia 'feedback' sia 'is_positive' — non posso definire positività.")

    required = {"user_id", "id_recommendation", "feedback_positive"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Mancano colonne richieste nel dataset storico: {missing}")

    return df

# ---------- Pipeline ----------
def generate_for_domain(spark: SparkSession, domain: str):
    print("\n===================================================")
    print(f"🚀 AVVIO GENERAZIONE RACCOMANDAZIONI: {domain.upper()}")
    print("===================================================")

    config = CONFIG[domain]

    # FASE 1: Candidate Generation
    print(f"[{domain}] 1) Carico dati storici → {config['historical_data_path']}")
    historical_df = spark.read.parquet(config['historical_data_path'])
    if historical_df.rdd.isEmpty():
        print(f"[{domain}] ⚠️ Nessun dato storico disponibile. Skip dominio.")
        return

    print(f"[{domain}] 1) Normalizzo schema storico (id_recommendation, feedback_positive)...")
    historical_df = normalize_schema_for_history(historical_df)

    print(f"[{domain}] 1) Carico mappa cluster → {config['user_cluster_map_path']}")
    user_cluster_map_df = spark.read.parquet(config['user_cluster_map_path'])
    if user_cluster_map_df.rdd.isEmpty():
        print(f"[{domain}] ⚠️ Nessuna mappa utente→cluster. Skip dominio.")
        return

    print(f"[{domain}] 1) Candidati: raccomandazioni con feedback positivo per cluster...")
    popular_recs_per_cluster_df = (
        historical_df.filter(col("feedback_positive") == True)
        .join(user_cluster_map_df, "user_id")
        .groupBy("cluster_id")
        .agg(collect_set("id_recommendation").alias("candidate_recs"))
    )

    if popular_recs_per_cluster_df.rdd.isEmpty():
        print(f"[{domain}] ⚠️ Nessun candidato trovato. Skip dominio.")
        return

    print(f"[{domain}] ✅ Esempio candidati per cluster:")
    popular_recs_per_cluster_df.show(truncate=False)

    # FASE 2: Preparazione input inferenza
    print(f"[{domain}] 2) Carico catalogo raccomandazioni → {config['recommendations_path']}")
    recommendations_catalog_df = spark.read.parquet(config['recommendations_path'])
    if recommendations_catalog_df.rdd.isEmpty():
        print(f"[{domain}] ⚠️ Catalogo raccomandazioni vuoto. Skip dominio.")
        return

    # Assicuriamoci che il catalogo abbia id_recommendation
    if "id_recommendation" not in recommendations_catalog_df.columns and "recommendation_id" in recommendations_catalog_df.columns:
        recommendations_catalog_df = recommendations_catalog_df.withColumnRenamed("recommendation_id", "id_recommendation")

    print(f"[{domain}] 2) Costruisco coppie (utente, raccomandazione) da valutare...")
    live_users_df = user_cluster_map_df
    inference_input_df = (
        live_users_df.join(popular_recs_per_cluster_df, "cluster_id")
        .withColumn("id_recommendation", explode(col("candidate_recs")))
        .join(recommendations_catalog_df, "id_recommendation")
    )

    total_pairs = inference_input_df.count()
    print(f"[{domain}] 🔢 Numero coppie (utente, raccomandazione) = {total_pairs}")
    if total_pairs == 0:
        print(f"[{domain}] ⚠️ Nessuna coppia da valutare. Skip dominio.")
        return

    # FASE 3: Inferenza per cluster
    print(f"[{domain}] 3) Eseguo inferenza con i modelli per cluster...")
    all_predictions = []
    for i in range(K_CLUSTERS):
        print(f"[{domain}]   • Cluster {i}: preparo input e carico modello...")
        cluster_inference_df = inference_input_df.filter(col("cluster_id") == i)
        if cluster_inference_df.rdd.isEmpty():
            print(f"[{domain}]     → Nessun utente/candidato per cluster {i}. Salto.")
            continue

        model_path = f"{config['classification_models_path']}cluster_{i}"
        print(f"[{domain}]     → Model path: {model_path}")
        classification_model = PipelineModel.load(model_path)

        preds = classification_model.transform(cluster_inference_df)
        n_preds = preds.count()
        print(f"[{domain}]     → Predizioni ottenute: {n_preds}")
        if n_preds > 0:
            all_predictions.append(preds)

    if not all_predictions:
        print(f"[{domain}] ❌ Nessuna predizione generata. Skip dominio.")
        return

    # FASE 4: Ranking & salvataggio
    print(f"[{domain}] 4) Unisco predizioni e calcolo ranking...")
    full_predictions_df = all_predictions[0]
    for df in all_predictions[1:]:
        full_predictions_df = full_predictions_df.unionByName(df)

    predictions_with_prob = full_predictions_df.withColumn(
        "success_prob", element_at(vector_to_array(col("probability")), 2)  # 1-based
    )

    window_spec = Window.partitionBy("user_id").orderBy(col("success_prob").desc())
    ranked_recommendations_df = predictions_with_prob.withColumn("rank", row_number().over(window_spec))
    top_recs_df = ranked_recommendations_df.filter(col("rank") <= TOP_N_RECS)

    print(f"[{domain}] 🏆 Top {TOP_N_RECS} raccomandazioni per alcuni utenti:")
    top_recs_df.orderBy("user_id", "rank").show(20, truncate=False)

    # Esempio salvataggio (se vuoi persistere):
    # out_path = f"{config['recommendations_path']}ranked_top_{TOP_N_RECS}"
    # print(f"[{domain}] 💾 Salvo ranking in: {out_path}")
    # top_recs_df.write.mode("overwrite").parquet(out_path)

    print(f"[{domain}] ✅ Generazione raccomandazioni COMPLETATA.")

# ---------- Main ----------
if __name__ == "__main__":
    print("\n=== START JOB: RECOMMENDATIONS (WORKOUT → NUTRITION) ===")
    spark = get_spark_session()
    try:
        generate_for_domain(spark, "workout")
        generate_for_domain(spark, "nutrition")
        print("\n🎉 TUTTO COMPLETATO: workout ✓  |  nutrition ✓")
    finally:
        spark.stop()
        print("🧹 SparkSession chiusa.")
