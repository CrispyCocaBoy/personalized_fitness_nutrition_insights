# spark_scripts/generate_recommendations.py
import os
import psycopg
from typing import List, Iterable, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType
from pyspark.ml.pipeline import PipelineModel
import time

CONFIG = {
    "nutrition": {
        "user_cluster_map_path": "s3a://gold/models/nutrition/user_cluster_map/",
        "recommendations_path": "s3a://gold/nutrition/recommendations/",
        "classification_models_path": "s3a://gold/models/nutrition/classification_models/",
        "historical_data_path": "s3a://gold/nutrition/training_data/",
        "db_table": "user_nutrition_rankings",
        "user_feature_cols": [
            "gender", "age", "height", "avg_weight_last7d", "bmi",
            "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
            "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
        ],
        "rec_catalog_mappings": {
            "recommendation_type": {"fallback_from": ["type", "category"]}
        }
    },
    "workout": {
        "user_cluster_map_path": "s3a://gold/models/workout/user_cluster_map/",
        "recommendations_path": "s3a://gold/workout/recommendations/",
        "classification_models_path": "s3a://gold/models/workout/classification_models/",
        "historical_data_path": "s3a://gold/workout/training_data/",
        "db_table": "user_workout_rankings",
        "user_feature_cols": [
            "gender", "age", "height", "avg_weight_last7d", "bmi",
            "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
        ],
        "rec_catalog_mappings": {
            "type": {"fallback_from": ["category"]},
            "difficulty": {"fallback_from": []},
            "duration": {"fallback_from": ["duration_minutes"]},
        }
    }
}

K_CLUSTERS = 4
TOP_N_RECS = 10

# ---- Spark ----
def get_spark_session():
    builder = SparkSession.builder.appName("RecommendationPipeline")
    s3_endpoint   = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_path_style = os.getenv("S3_PATH_STYLE", "true")
    s3_ssl        = os.getenv("S3_SSL_ENABLED", "false")
    aws_key     = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret  = os.getenv("AWS_SECRET_ACCESS_KEY")
    minio_key   = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret= os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    builder = (builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", s3_path_style)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_ssl)
        .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("S3_REGION", "us-east-1"))
    )
    if aws_key and aws_secret:
        builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                 "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider")
    else:
        builder = (builder
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", minio_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        )
    return builder.getOrCreate()

# ---- Cockroach (psycopg) ----
def connection():
    host = os.getenv("CRDB_HOST", "cockroachdb")
    port = int(os.getenv("CRDB_PORT", "26257"))
    db   = os.getenv("CRDB_DB",   "user_device_db")
    user = os.getenv("CRDB_USER", "root")
    pwd  = os.getenv("CRDB_PASSWORD")
    kwargs = dict(host=host, port=port, dbname=db, user=user)
    if pwd: kwargs["password"] = pwd
    return psycopg.connect(**kwargs)

def load_live_user_ids_from_db() -> List[int]:
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users;")
        return [int(r[0]) for r in cur.fetchall()]

# ---- Staging persistente (no temp) ----
def _ensure_staging_exists(conn, target_tbl: str) -> str:
    """
    Crea una staging persistente {target_tbl}_stg se non esiste.
    1) Prova CREATE TABLE IF NOT EXISTS ... LIKE target
    2) Se LIKE non è supportato, fallback a CTAS con 0 righe (niente PK/FK, ma ok per staging)
    """
    stg = f"{target_tbl}_stg"
    with conn.cursor() as cur:
        try:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {stg} (LIKE {target_tbl});")
        except Exception:
            # Fallback: CTAS (crea solo colonne/nomi; niente vincoli)
            cur.execute(f"CREATE TABLE IF NOT EXISTS {stg} AS SELECT * FROM {target_tbl} LIMIT 0;")
    return stg

def _clear_staging(conn, stg_tbl: str):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {stg_tbl};")  # pulizia sicura (niente FK in staging)

def _bulk_insert(conn, tbl: str, rows: Iterable[Tuple[int, int, str, str, str, float, int]], batch_size: int = 1000):
    """
    Inserisce in bulk nella tabella indicata (staging o target).
    Schema atteso: (user_id, cluster_id, recommendation_id, title, description, success_prob, rank)
    """
    sql = (f"INSERT INTO {tbl} "
           "(user_id, cluster_id, recommendation_id, title, description, success_prob, rank) "
           "VALUES (%s, %s, %s, %s, %s, %s, %s)")
    with conn.cursor() as cur:
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)

def _upsert_from_staging(conn, target_tbl: str, stg_tbl: str):
    """
    UPSERT (INSERT ... ON CONFLICT DO UPDATE) dalla staging alla target.
    """
    upsert_sql = f"""
    INSERT INTO {target_tbl} (user_id, cluster_id, recommendation_id, title, description, success_prob, rank)
    SELECT user_id, cluster_id, recommendation_id, title, description, success_prob, rank
    FROM {stg_tbl}
    ON CONFLICT (user_id, recommendation_id)
    DO UPDATE SET
        cluster_id   = EXCLUDED.cluster_id,
        title        = EXCLUDED.title,
        description  = EXCLUDED.description,
        success_prob = EXCLUDED.success_prob,
        rank         = EXCLUDED.rank;
    """
    with conn.cursor() as cur:
        cur.execute(upsert_sql)

def _cleanup_obsolete(conn, target_tbl: str, stg_tbl: str):
    """
    Elimina dalla target SOLO le coppie (user_id, recommendation_id) non presenti nella staging,
    limitandosi agli utenti presenti nella staging. Le relative righe di feedback cadranno per FK CASCADE.
    """
    delete_sql = f"""
    DELETE FROM {target_tbl} t
    WHERE t.user_id IN (SELECT DISTINCT user_id FROM {stg_tbl})
      AND NOT EXISTS (
            SELECT 1
            FROM {stg_tbl} s
            WHERE s.user_id = t.user_id
              AND s.recommendation_id = t.recommendation_id
      );
    """
    with conn.cursor() as cur:
        cur.execute(delete_sql)

def write_rankings_atomic(target_tbl: str, rows: Iterable[Tuple[int, int, str, str, str, float, int]]):
    """
    Scrittura ATOMICA con staging persistente:
      1) ensure staging {target}_stg
      2) clear staging
      3) bulk insert staging
      4) upsert -> target
      5) cleanup obsolete per gli utenti toccati
    """
    with connection() as conn:
        try:
            stg_tbl = _ensure_staging_exists(conn, target_tbl)
            _clear_staging(conn, stg_tbl)
            _bulk_insert(conn, stg_tbl, rows)
            _upsert_from_staging(conn, target_tbl, stg_tbl)
            _cleanup_obsolete(conn, target_tbl, stg_tbl)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

# ---- Utils ----
def iter_rows(df: DataFrame):
    cols = ["user_id", "cluster_id", "recommendation_id", "title", "description", "success_prob", "rank"]
    for r in df.select(*cols).toLocalIterator():
        yield (
            int(r["user_id"]) if r["user_id"] is not None else None,
            int(r["cluster_id"]) if r["cluster_id"] is not None else None,
            str(r["recommendation_id"]) if r["recommendation_id"] is not None else "",
            r["title"] if r["title"] is not None else None,
            r["description"] if r["description"] is not None else None,
            float(r["success_prob"]) if r["success_prob"] is not None else None,
            int(r["rank"]) if r["rank"] is not None else None,
        )

def ensure_columns_for_domain(df, domain_cfg):
    for target_col, mapping in domain_cfg.get("rec_catalog_mappings", {}).items():
        if target_col not in df.columns:
            for fb in mapping.get("fallback_from", []):
                if fb in df.columns:
                    df = df.withColumn(target_col, F.col(fb))
                    break
    return df

# ---- Pipeline per dominio ----
def run_for_domain(spark: SparkSession, domain: str):
    print(f"\n=== {domain.upper()} ===")
    cfg = CONFIG[domain]

    print("[LIVE] Carico utenti dal DB...")
    live_user_ids = load_live_user_ids_from_db()
    if not live_user_ids:
        print("[LIVE] Nessun utente live. STOP.")
        return
    live_users_df = spark.createDataFrame([(int(uid),) for uid in live_user_ids],
                                          StructType([StructField("user_id", IntegerType(), True)]))

    print("[LOAD] Leggo mappe/storico/catalogo...")
    user_cluster_map = spark.read.parquet(cfg["user_cluster_map_path"]).withColumn("user_id", F.col("user_id").cast(IntegerType()))
    historical_df   = spark.read.parquet(cfg["historical_data_path"]).withColumn("recommendation_id", F.col("recommendation_id").cast(StringType()))
    catalog_df      = ensure_columns_for_domain(spark.read.parquet(cfg["recommendations_path"]), cfg)\
                        .withColumn("recommendation_id", F.col("recommendation_id").cast(StringType()))
    live_users_df   = live_users_df.withColumn("user_id", F.col("user_id").cast(IntegerType()))

    live_user_map = (live_users_df.alias("u")
                     .join(user_cluster_map.alias("m"), "user_id", "inner")
                     .select("user_id", "cluster_id").dropDuplicates())
    if live_user_map.rdd.isEmpty():
        print("[LIVE] Nessuna mappa user->cluster. STOP."); return

    popular_recs_per_cluster = (historical_df.filter(F.col("is_positive") == 1)
                                .join(user_cluster_map.select("user_id","cluster_id"), "user_id")
                                .groupBy("cluster_id")
                                .agg(F.collect_set("recommendation_id").alias("candidate_recs")))
    if popular_recs_per_cluster.rdd.isEmpty():
        print("[CANDIDATES] Nessuna raccomandazione positiva. STOP."); return

    user_feat_cols = ["user_id","date"] + [c for c in cfg["user_feature_cols"] if c in historical_df.columns]
    latest_w = Window.partitionBy("user_id").orderBy(F.col("date").desc())
    latest_user_features = (historical_df.select(*[c for c in user_feat_cols if c in historical_df.columns])
                            .withColumn("rn", F.row_number().over(latest_w))
                            .filter(F.col("rn")==1).drop("rn","date").dropDuplicates(["user_id"]))
    if latest_user_features.rdd.isEmpty():
        print("[FEATURES] Nessuna feature utente. STOP."); return

    inference_input = (live_user_map.alias("um")
                       .join(popular_recs_per_cluster.alias("pc"), "cluster_id")
                       .withColumn("recommendation_id", F.explode("pc.candidate_recs"))
                       .join(catalog_df.alias("cat"), "recommendation_id")
                       .join(latest_user_features.alias("uf"), "user_id")
                       .select("user_id","cluster_id","recommendation_id","title","description",
                               *[F.col(f"uf.{c}") for c in cfg["user_feature_cols"] if c in latest_user_features.columns],
                               *[F.col(f"cat.{c}") for c in ["recommendation_type","type","difficulty","duration","duration_minutes","category"] if c in catalog_df.columns]
                       ))

    if domain == "workout":
        if "type" not in inference_input.columns and "category" in inference_input.columns:
            inference_input = inference_input.withColumn("type", F.col("category"))
        if "duration" not in inference_input.columns and "duration_minutes" in inference_input.columns:
            inference_input = inference_input.withColumn("duration", F.col("duration_minutes"))

    total_pairs = inference_input.count()
    print(f"[INFERENCE] Coppie (user,rec) da valutare: {total_pairs}")
    if total_pairs == 0:
        print("[INFERENCE] Nessun candidato. STOP."); return

    def prob_pos(v):
        try: return float(v[1])
        except Exception: return None
    prob_pos_udf = F.udf(prob_pos, DoubleType())

    all_preds = []
    for i in range(K_CLUSTERS):
        print(f"[MODEL] cluster_{i}")
        cluster_input = inference_input.filter(F.col("cluster_id")==i)
        if cluster_input.rdd.isEmpty():
            print("  -> no data"); continue
        model_path = f"{cfg['classification_models_path']}cluster_{i}"
        try:
            model = PipelineModel.load(model_path)
            preds = model.transform(cluster_input).withColumn("success_prob", prob_pos_udf(F.col("probability")))
            all_preds.append(preds.select("user_id","cluster_id","recommendation_id","title","description","success_prob"))
        except Exception as e:
            print(f"  -> modello non disponibile/errore: {e}")
            continue

    if not all_preds:
        print("[INFERENCE] Nessuna predizione generata. STOP."); return

    full_preds = all_preds[0]
    for df in all_preds[1:]:
        full_preds = full_preds.unionByName(df)

    win = Window.partitionBy("user_id").orderBy(F.col("success_prob").desc_nulls_last())
    ranked = full_preds.withColumn("rank", F.row_number().over(win))
    top_recs = ranked.filter(F.col("rank") <= TOP_N_RECS).orderBy("user_id","rank")

    out_cnt = top_recs.count()
    print(f"[RESULT] {domain}: righe da inserire = {out_cnt}")
    top_recs.show(30, truncate=False)

    target_table = cfg["db_table"]
    if out_cnt == 0:
        print(f"[DB] {domain}: nessuna riga. SKIP write su {target_table}.")
        return

    print(f"[DB] {domain}: UPSERT + CLEANUP su {target_table} (staging persistente)...")
    write_rankings_atomic(target_table, iter_rows(top_recs))
    print(f"[DB] {domain}: DONE.")

def main():
    spark = get_spark_session()
    try:
        run_for_domain(spark, "nutrition")
        run_for_domain(spark, "workout")
        print("\nDONE: workout | nutrition")
    finally:
        spark.stop()
        print("SparkSession chiusa.")

if __name__ == "__main__":
    main()
