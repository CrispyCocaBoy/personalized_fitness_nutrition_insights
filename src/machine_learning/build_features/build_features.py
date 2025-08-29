# build_features.py
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, round, expr, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
)

# -------------------------
# Spark Session
# -------------------------
def get_spark_session():
    builder = SparkSession.builder.appName("FeatureEngineering")

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

# -------------------------
# Utils
# -------------------------
def calculate_bmi(df: DataFrame) -> DataFrame:
    """Aggiunge 'bmi' assumendo: height in cm, avg_weight_last7d in kg."""
    return df.withColumn(
        "bmi",
        round(col("avg_weight_last7d") / ((col("height") / 100.0) ** 2), 2)
    )

def process_recommendations_catalog(spark: SparkSession, source_path: str, destination_path: str):
    print(f"  - Processing recommendations from {source_path}")
    df = spark.read.option("header", "true").csv(source_path)
    df.write.mode("overwrite").parquet(destination_path)
    print(f"    -> Catalog saved to {destination_path}")

# -------------------------
# Nutrition (feedback)
# -------------------------
def process_nutrition_feedback(spark: SparkSession, bronze_base_path: str, gold_base_path: str):
    print("\n[2A] Processing Nutrition Feedback...")
    nutrition_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("id_cluster", IntegerType(), True),  # opzionale, verrà ignorato a valle
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("height", DoubleType(), True),  # numerico
        StructField("avg_weight_last7d", DoubleType(), True),  # numerico
        StructField("noted_at", TimestampType(), True),
        StructField("calories_consumed_last_3_days_avg", DoubleType(), True),
        StructField("protein_intake_last_3_days_avg", DoubleType(), True),
        StructField("carbs_intake_last_3_days_avg", DoubleType(), True),
        StructField("fat_intake_last_3_days_avg", DoubleType(), True),
        StructField("recommendation_id", IntegerType(), True),
        StructField("is_positive", IntegerType(), True),
    ])

    src = f"{bronze_base_path}/user_feedback_nutrition.csv"
    df = (spark.read
                .option("header", "true")
                .schema(nutrition_schema)
                .csv(src))

    # Normalizza eventuali virgole decimali
    numeric_cols = [
        "height", "avg_weight_last7d",
        "calories_consumed_last_3_days_avg",
        "protein_intake_last_3_days_avg",
        "carbs_intake_last_3_days_avg",
        "fat_intake_last_3_days_avg"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, regexp_replace(col(c).cast("string"), ",", ".").cast("double"))

    df = df.withColumn("date", to_date(col("noted_at")))
    df = calculate_bmi(df)

    print("\n[NUTRITION] Schema normalizzato:")
    df.printSchema()
    print("\n[NUTRITION] Sample righe:")
    df.show(5, truncate=False)

    dst = f"{gold_base_path}/nutrition/training_data"
    (df.write.mode("overwrite").partitionBy("date").parquet(dst))
    print(f"  -> Nutrition training data saved to {dst}")

# -------------------------
# Workout (feedback)
# -------------------------
def process_workout_feedback(spark: SparkSession, bronze_base_path: str, gold_base_path: str):
    print("\n[2B] Processing Workout Feedback...")
    workout_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("id_cluster", IntegerType(), True),  # opzionale, verrà ignorato a valle
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("height", DoubleType(), True),                 # numerico
        StructField("avg_weight_last7d", DoubleType(), True),      # numerico
        StructField("avg_steps_last7d", IntegerType(), True),
        StructField("avg_bpm_last7d", DoubleType(), True),         # numerico
        StructField("avg_active_minutes_last7d", IntegerType(), True),
        StructField("noted_at", TimestampType(), True),
        StructField("recommendation_id", IntegerType(), True),
        StructField("is_positive", IntegerType(), True),
    ])

    src = f"{bronze_base_path}/user_feedback_workout.csv"
    df = (spark.read
                .option("header", "true")
                .schema(workout_schema)
                .csv(src))

    # Normalizza eventuali virgole decimali
    for c in ["height", "avg_weight_last7d", "avg_bpm_last7d"]:
        if c in df.columns:
            df = df.withColumn(c, regexp_replace(col(c).cast("string"), ",", ".").cast("double"))

    df = df.withColumn("date", to_date(col("noted_at")))
    df = calculate_bmi(df)

    print("\n[WORKOUT] Schema normalizzato:")
    df.printSchema()
    print("\n[WORKOUT] Sample righe:")
    df.show(5, truncate=False)

    dst = f"{gold_base_path}/workout/training_data"
    (df.write.mode("overwrite").partitionBy("date").parquet(dst))
    print(f"  -> Workout training data saved to {dst}")

# -------------------------
# Workout (live features da daily metrics)
# -------------------------
def process_workout_live_features(spark: SparkSession, bronze_base_path: str, gold_base_path: str):
    print("\n[3] Building 'live' workout features from daily metrics...")
    metrics = (spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(f"{bronze_base_path}/daily_user_metrics_live.csv")
                    .withColumn("date", to_date(col("date"))))

    window_spec = Window.partitionBy("user_id").orderBy(col("date").cast("long")).rowsBetween(-7, -1)

    features_df = (metrics
        .withColumn("avg_steps_last7d", round(expr("avg(total_steps)").over(window_spec)))
        .withColumn("avg_bpm_last7d", round(expr("avg(avg_bpm)").over(window_spec)))
        .withColumn("avg_active_minutes_last7d", round(expr("avg(active_minutes)").over(window_spec)))
    )

    dst = f"{gold_base_path}/workout/features_live"
    (features_df
        .select("user_id", "date", "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d")
        .na.drop()
        .write.mode("overwrite").partitionBy("date").parquet(dst))
    print(f"  -> Live workout features saved to {dst}")

# -------------------------
# Orchestrazione
# -------------------------
def process_data(spark: SparkSession, bronze_base_path: str, gold_base_path: str):
    print("\n--- Inizio Elaborazione Dati ---")

    # [1] Cataloghi
    print("\n[1] Processing Catalogs...")
    process_recommendations_catalog(
        spark,
        f"{bronze_base_path}/nutrition_recommendations.csv",
        f"{gold_base_path}/nutrition/recommendations"
    )
    process_recommendations_catalog(
        spark,
        f"{bronze_base_path}/workout_recommendations.csv",
        f"{gold_base_path}/workout/recommendations"
    )

    # [2] Feedback
    process_nutrition_feedback(spark, bronze_base_path, gold_base_path)
    process_workout_feedback(spark, bronze_base_path, gold_base_path)

    # [3] Live features (workout)
    process_workout_live_features(spark, bronze_base_path, gold_base_path)

    print("\n--- Fine Elaborazione Dati ---")

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    spark = get_spark_session()

    BRONZE_S3_PATH = "s3a://bronze"
    GOLD_S3_PATH = "s3a://gold"

    process_data(spark, BRONZE_S3_PATH, GOLD_S3_PATH)

    spark.stop()
    print("\n--- Esecuzione Job PySpark Completata ---")
