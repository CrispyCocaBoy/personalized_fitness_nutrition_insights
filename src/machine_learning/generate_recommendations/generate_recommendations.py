# spark_scripts/generate_recommendations.py

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, row_number, collect_set, explode, trim,
    regexp_extract, regexp_replace, lit
)
from pyspark.sql.window import Window
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.functions import vector_to_array  # <-- per estrarre p(classe=1) lato JVM

# =============== CONFIG ===============
CONFIG = {
    "nutrition": {
        "user_cluster_map_path": "s3a://gold/models/nutrition/user_cluster_map/",
        "recommendations_path": "s3a://gold/nutrition/recommendations/",
        "classification_models_path": "s3a://gold/models/nutrition/classification_models/",
        "historical_data_path": "s3a://gold/nutrition/training_data/",
        "training_data_path": "s3a://gold/nutrition/training_data/",
        "output_path": "s3a://gold/nutrition/generated_recommendations/"
    },
    "workout": {
        "user_cluster_map_path": "s3a://gold/models/workout/user_cluster_map/",
        "recommendations_path": "s3a://gold/workout/recommendations/",
        "classification_models_path": "s3a://gold/models/workout/classification_models/",
        "historical_data_path": "s3a://gold/workout/training_data/",
        "training_data_path": "s3a://gold/workout/training_data/",
        "output_path": "s3a://gold/workout/generated_recommendations/"
    }
}
K_CLUSTERS = 4
TOP_N_RECS = int(os.getenv("TOP_N_RECS", "10"))

# =============== SPARK & S3 ===============
def get_spark_session():
    builder = SparkSession.builder.appName("generate_recommendations")

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
            builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                   .config("spark.hadoop.fs.s3a.access.key", minio_key)
                   .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        )

    return builder.getOrCreate()


def path_exists(spark, uri: str) -> bool:
    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(uri)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(p.toUri(), conf)
        return fs.exists(p)
    except Exception:
        return False

# =============== SCHEMA HELPERS ===============
def to_double_from_messy_string(col_expr):
    # Estrae numero (con . o ,) e lo cast a double
    num = regexp_extract(trim(col_expr), r'(\d+(?:[.,]\d+)?)', 1)
    return regexp_replace(num, ",", ".").cast("double")

def normalize_training_schema(df):
    # uniforma la chiave id raccomandazione nello storico
    if "id_recommendation" not in df.columns and "recommendation_id" in df.columns:
        df = df.withColumnRenamed("recommendation_id", "id_recommendation")
    # crea label se manca (da feedback o is_positive)
    if "label" not in df.columns:
        from pyspark.sql.functions import when
        if "feedback" in df.columns:
            df = df.withColumn("label", when(col("feedback") == "positive", 1).otherwise(0))
        elif "is_positive" in df.columns:
            df = df.withColumn(
                "label",
                when((col("is_positive") == True) | (col("is_positive") == 1) |
                     (col("is_positive") == "true") | (col("is_positive") == "1"), 1).otherwise(0)
            )
    return df

def normalize_catalog_schema(df, domain):
    # Allinea chiave primaria
    if "id_recommendation" not in df.columns and "recommendation_id" in df.columns:
        df = df.withColumnRenamed("recommendation_id", "id_recommendation")
    # Normalizzazioni specifiche
    if domain == "workout":
        if "type" not in df.columns and "category" in df.columns:
            df = df.withColumn("type", col("category"))
        if "duration" not in df.columns and "duration_minutes" in df.columns:
            df = df.withColumn("duration", col("duration_minutes"))
        dtypes = dict(df.dtypes)
        if "duration" in dtypes and dtypes["duration"] == "string":
            df = df.withColumn("duration", to_double_from_messy_string(col("duration")))
    if domain == "nutrition":
        if "recommendation_type" not in df.columns and "type" in df.columns:
            df = df.withColumnRenamed("type", "recommendation_type")
    return df

def build_user_latest_features(spark, domain, cfg):
    """
    Ultimo record per utente dal training_data del dominio.
    Restituisce le colonne utente che il modello si aspetta.
    """
    td = spark.read.parquet(cfg['training_data_path'])
    if td.rdd.isEmpty():
        return None

    w = Window.partitionBy("user_id").orderBy(col("date").desc())
    td_last = td.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    if domain == "nutrition":
        user_feats = [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
            "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
        ]
    else:
        user_feats = [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
        ]

    needed = set(user_feats) | {
        "user_id", "gender", "age", "height", "avg_weight_last7d", "bmi",
        "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d",
        "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
        "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
    }
    present = [c for c in td_last.columns if c in needed]

    # assicurati che 'gender' (grezzo) ci sia per eventuali indexer nel modello
    if "gender" not in present and "gender" in td_last.columns:
        present.append("gender")

    return td_last.select(*present)

# =============== PIPELINE PER DOMINIO ===============
def generate_for_domain(spark, domain: str):
    print("\n===================================================")
    print(f"AVVIO GENERAZIONE RACCOMANDAZIONI: {domain.upper()}")
    print("===================================================")

    cfg = CONFIG[domain]

    # 1) Candidate generation (positivi per cluster)
    print("Fase 1: Candidate generation (positivi per cluster)...")
    historical_df = spark.read.parquet(cfg['historical_data_path'])
    if historical_df.rdd.isEmpty():
        print(f"[{domain}] Storico vuoto. Stop.")
        return
    historical_df = normalize_training_schema(historical_df)

    user_cluster_map_df = spark.read.parquet(cfg['user_cluster_map_path'])
    if user_cluster_map_df.rdd.isEmpty():
        print(f"[{domain}] user_cluster_map vuoto. Stop.")
        return

    if "label" in historical_df.columns:
        positives_df = historical_df.filter(col("label") == 1)
    elif "feedback" in historical_df.columns:
        positives_df = historical_df.filter(col("feedback") == "positive")
    else:
        print(f"[{domain}] Nessun segnale di positività (label/feedback). Stop.")
        return

    popular_recs_per_cluster_df = (
        positives_df.join(user_cluster_map_df, "user_id")
                    .groupBy("cluster_id")
                    .agg(collect_set("id_recommendation").alias("candidate_recs"))
    )
    popular_recs_per_cluster_df.show(truncate=False)

    # 2) Preparazione input per inferenza
    print("\nFase 2: Preparazione input per inferenza...")
    user_latest = build_user_latest_features(spark, domain, cfg)
    if user_latest is None or user_latest.rdd.isEmpty():
        print(f"[{domain}] Nessun profilo utente disponibile. Stop.")
        return

    recommendations_catalog_df = spark.read.parquet(cfg['recommendations_path'])
    if recommendations_catalog_df.rdd.isEmpty():
        print(f"[{domain}] Catalogo vuoto. Stop.")
        return
    recommendations_catalog_df = normalize_catalog_schema(recommendations_catalog_df, domain)

    inference_input_df = (
        user_cluster_map_df
        .join(user_latest, "user_id")
        .join(popular_recs_per_cluster_df, "cluster_id", "left")
        .withColumn("id_recommendation", explode(col("candidate_recs")))
        .join(recommendations_catalog_df, "id_recommendation")
    )

    total_pairs = inference_input_df.count()
    print(f"Numero di coppie (utente, raccomandazione) da valutare: {total_pairs}")
    if total_pairs == 0:
        print(f"[{domain}] Nessun candidato da valutare. Stop.")
        return

    # 3) Inferenza per cluster
    print("\nFase 3: Inferenza per cluster...")
    all_predictions = []
    for i in range(K_CLUSTERS):
        model_path = f"{cfg['classification_models_path']}cluster_{i}"
        print(f"  - Cluster {i}: cerco modello in {model_path}")
        if not path_exists(spark, model_path):
            print(f"    -> Modello assente, skip cluster {i}.")
            continue

        try:
            classification_model = PipelineModel.load(model_path)
        except Exception as e:
            print(f"    -> Errore load modello cluster {i}: {e}. Skip.")
            continue

        cluster_inference_df = inference_input_df.filter(col("cluster_id") == i)
        if cluster_inference_df.rdd.isEmpty():
            print(f"    -> Nessun record per cluster {i}. Skip.")
            continue

        preds = classification_model.transform(cluster_inference_df)
        # estrai p(classe=1) dalla colonna VectorUDT 'probability' senza UDF Python
        preds = preds.withColumn("success_prob", vector_to_array(col("probability"))[1])

        all_predictions.append(
            preds.select("user_id", "cluster_id", "id_recommendation",
                         "title", "description", "success_prob")
        )

    if not all_predictions:
        print(f"[{domain}] Nessuna predizione generata (modelli mancanti?). Stop.")
        return

    # 4) Ranking & Salvataggio
    print("\nFase 4: Ranking e salvataggio Top-N...")
    full_predictions_df = all_predictions[0]
    for df in all_predictions[1:]:
        full_predictions_df = full_predictions_df.unionByName(df)

    w = Window.partitionBy("user_id").orderBy(col("success_prob").desc())
    ranked = full_predictions_df.withColumn("rank", row_number().over(w))
    topn = ranked.filter(col("rank") <= TOP_N_RECS).select(
        "user_id", "id_recommendation", "title", "description",
        "success_prob", "rank", current_timestamp().alias("generated_at")
    )

    topn.show(20, truncate=False)

    out_path = cfg["output_path"]
    print(f"[{domain}] Salvo Top-{TOP_N_RECS} → {out_path}")
    topn.write.mode("overwrite").parquet(out_path)

    print(f"[{domain}] Generazione completata.")

# =============== MAIN ===============
if __name__ == "__main__":
    print("\n=== START: GENERATE RECOMMENDATIONS (WORKOUT → NUTRITION) ===")
    spark = get_spark_session()
    try:
        generate_for_domain(spark, "workout")
        generate_for_domain(spark, "nutrition")
        print("\nDONE: workout | nutrition")
    finally:
        spark.stop()
        print("SparkSession chiusa.")
