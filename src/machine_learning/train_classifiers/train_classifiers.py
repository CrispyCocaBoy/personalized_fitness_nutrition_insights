import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract, regexp_replace, trim
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# --- CONFIG ---
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
MIN_ROWS_PER_CLUSTER = int(os.getenv("MIN_ROWS_PER_CLUSTER", "20"))
DEBUG_S3 = os.getenv("DEBUG_S3", "0") == "1"


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


# ---------- S3 debug helpers ----------
def print_s3_conf(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    ep = conf.get("fs.s3a.endpoint")
    prov = conf.get("fs.s3a.aws.credentials.provider")
    ak = conf.get("fs.s3a.access.key")
    pstyle = conf.get("fs.s3a.path.style.access")
    ssl = conf.get("fs.s3a.connection.ssl.enabled")
    region = conf.get("fs.s3a.endpoint.region")
    print(f"[S3A] endpoint={ep} region={region} path_style={pstyle} ssl={ssl} provider={prov} access_key={ak}")


def hadoop_ls(spark, uri: str):
    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(uri)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(path.toUri(), conf)
        if not fs.exists(path):
            print(f"[S3A] ❌ Path non esiste: {uri}")
            return
        print(f"[S3A] 📂 Listing di {uri}:")
        for status in fs.listStatus(path):
            print("   -", status.getPath().toString())
    except Exception as e:
        print(f"[S3A] (ls) errore su {uri}: {e}")


def write_marker_json(spark, uri: str, content: dict):
    df = spark.createDataFrame([content])
    df.write.mode("overwrite").json(uri)
    if DEBUG_S3:
        hadoop_ls(spark, uri)


# ---------- Helpers ----------
def normalize_schema(training_df, rec_df):
    # id raccomandazione allineato
    if "id_recommendation" not in training_df.columns and "recommendation_id" in training_df.columns:
        training_df = training_df.withColumnRenamed("recommendation_id", "id_recommendation")
    if "id_recommendation" not in rec_df.columns and "recommendation_id" in rec_df.columns:
        rec_df = rec_df.withColumnRenamed("recommendation_id", "id_recommendation")

    # label da feedback / is_positive
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


def to_double_from_messy_string(col_expr):
    num = regexp_extract(trim(col_expr), r'(\d+(?:[.,]\d+)?)', 1)
    num = regexp_replace(num, ",", ".").cast("double")
    return num


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

        # Se 'duration' è stringa, convertila a double
        if "duration" in master_df.columns:
            if dict(master_df.dtypes).get("duration") != "double":
                print("[workout] ⛏️ Converto 'duration' a double (pulizia stringhe)")
                master_df = master_df.withColumn("duration", to_double_from_messy_string(col("duration")))

    # --- DEBUG: ispezione schema e sample prima del training ---
    print(f"[{domain}] 🧪 master_df.printSchema():")
    master_df.printSchema()

    print(f"[{domain}] 🧾 master_df.sample (5 righe):")
    master_df.select(
        "user_id", "cluster_id", "id_recommendation",
        "gender", "age", "height", "avg_weight_last7d", "bmi",
        *[c for c in ["avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d", "type", "difficulty", "duration"] if c in master_df.columns],
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

    # 4) Feature list: usa solo quelle davvero presenti (+ cast robusto se stringhe numeriche)
    feature_cols = cfg['user_features'] + cfg['rec_features']
    dtypes = dict(master_df.dtypes)
    numeric_like = [c for c in feature_cols if c in master_df.columns]
    skip_cast = {"type_indexed", "difficulty_indexed", "recommendation_type_indexed", "gender_indexed"}
    for c in numeric_like:
        if c in skip_cast:
            continue
        if dtypes.get(c) == "string":
            print(f"[{domain}] ⛏️ Converto '{c}' da string a double")
            master_df = master_df.withColumn(c, to_double_from_messy_string(col(c)))

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
            marker = f"{cfg['model_output_path']}cluster_{i}/_SKIPPED"
            write_marker_json(spark, marker, {"skipped": True, "reason": "few_rows", "rows": int(n_rows)})
            continue

        # check classi
        n_classes = cluster_df.select("label").distinct().count()
        print(f"[{domain}]   • Classi distinte nel cluster {i}: {n_classes}")
        if n_classes < 2:
            print(f"[{domain}]   ⚠️ Solo una classe presente (label). Skip cluster {i}.")
            marker = f"{cfg['model_output_path']}cluster_{i}/_SKIPPED"
            write_marker_json(spark, marker, {"skipped": True, "reason": "single_class"})
            continue

        train_df, test_df = cluster_df.randomSplit([0.8, 0.2], seed=42)
        n_tr, n_te = train_df.count(), test_df.count()
        print(f"[{domain}]   • Split: train={n_tr}, test={n_te}")

        n_classes_test = test_df.select("label").distinct().count() if n_te > 0 else 0
        if n_te > 0 and n_classes_test < 2:
            print(f"[{domain}]   ℹ️ Test set con una sola classe. Salto AUC.")

        print(f"[{domain}]   🤖 Addestro RandomForest...")
        model = pipeline.fit(train_df)

        if n_te > 0 and n_classes_test >= 2:
            preds = model.transform(test_df)
            evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                                      metricName="areaUnderROC")
            auc = evaluator.evaluate(preds)
            print(f"[{domain}]   ✅ AUC cluster {i}: {auc:.4f}")
        else:
            print(f"[{domain}]   ℹ️ AUC non calcolata.")

        print(f"[{domain}]   ♻️ Refit su tutto il cluster e salvo modello...")
        final_model = pipeline.fit(cluster_df)
        model_path = f"{cfg['model_output_path']}cluster_{i}"
        print(f"[{domain}]   💾 Salvataggio: {model_path}")
        final_model.write().overwrite().save(model_path)

        if DEBUG_S3:
            hadoop_ls(spark, model_path)

    print(f"\n[{domain}] 🎉 TRAINING COMPLETATO.")


# ---------- Main ----------
if __name__ == "__main__":
    print("\n=== START JOB: TRAIN CLASSIFIERS (WORKOUT → NUTRITION) ===")
    spark = get_spark_session()
    if DEBUG_S3:
        print_s3_conf(spark)
    try:
        train_for_domain(spark, "workout")
        train_for_domain(spark, "nutrition")
        print("\n✅ DONE: workout ✓  |  nutrition ✓")
    finally:
        spark.stop()
        print("🧹 SparkSession chiusa.")
