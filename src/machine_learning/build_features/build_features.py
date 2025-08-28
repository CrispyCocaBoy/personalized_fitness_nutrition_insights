# build_features.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, round, expr
from pyspark.sql.window import Window
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, \
    TimestampType  # Per definire lo schema


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

def calculate_bmi(df: DataFrame) -> DataFrame:
    """Aggiunge una colonna 'bmi' a un DataFrame che contiene 'avg_weight_last7d' e 'height'."""
    print("    - Calculating BMI...")
    return df.withColumn("bmi", round(col("avg_weight_last7d") / ((col("height") / 100) ** 2), 2))


def process_recommendations_catalog(spark: SparkSession, source_path: str, destination_path: str):
    """Legge un catalogo di raccomandazioni in CSV e lo salva in Parquet."""
    print(f"  - Processing recommendations from {source_path}")
    df = spark.read.option("header", "true").csv(source_path)
    # Potremmo aggiungere qui logica più complessa se necessario (es. rinominare colonne dinamicamente)
    df.write.mode("overwrite").parquet(destination_path)
    print(f"    -> Catalog saved to {destination_path}")


def process_data(spark: SparkSession, bronze_base_path: str, gold_base_path: str):
    """
    Esegue l'intero processo di feature engineering per dati di nutrizione e workout.
    """
    print("\n--- Inizio Elaborazione Dati ---")

    # --- FASE 1: CATALOGHI ---
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

    # --- FASE 2: DATI DI FEEDBACK (NUTRITION & WORKOUT) ---
    print("\n[2] Processing User Feedback Data...")

    # Nutrition
    feedback_nutrition_df = spark.read.option("header", "true").option("inferSchema", "true").csv(
        f"{bronze_base_path}/user_feedback_nutrition.csv")
    feedback_nutrition_df = feedback_nutrition_df.withColumn("date", to_date(col("noted_at")))
    feedback_nutrition_df = calculate_bmi(feedback_nutrition_df)

    nutrition_training_path = f"{gold_base_path}/nutrition/training_data"
    feedback_nutrition_df.write.mode("overwrite").partitionBy("date").parquet(nutrition_training_path)
    print(f"  -> Nutrition training data saved to {nutrition_training_path}")

    # Workout
    feedback_workout_df = spark.read.option("header", "true").option("inferSchema", "true").csv(
        f"{bronze_base_path}/user_feedback_workout.csv")
    feedback_workout_df = feedback_workout_df.withColumn("date", to_date(col("noted_at")))
    feedback_workout_df = calculate_bmi(feedback_workout_df)

    workout_training_path = f"{gold_base_path}/workout/training_data"
    feedback_workout_df.write.mode("overwrite").partitionBy("date").parquet(workout_training_path)
    print(f"  -> Workout training data saved to {workout_training_path}")

    # --- FASE 3: FEATURE 'LIVE' DALLE METRICHE GIORNALIERE (WORKOUT) ---
    print("\n[3] Building 'live' workout features from daily metrics...")
    metrics_df = spark.read.option("header", "true").option("inferSchema", "true").csv(
        f"{bronze_base_path}/daily_user_metrics_live.csv") \
        .withColumn("date", to_date(col("date")))

    window_spec = Window.partitionBy("user_id").orderBy(col("date").cast("long")).rowsBetween(-7, -1)

    features_df = metrics_df \
        .withColumn("avg_steps_last7d", round(expr("avg(total_steps)").over(window_spec))) \
        .withColumn("avg_bpm_last7d", round(expr("avg(avg_bpm)").over(window_spec))) \
        .withColumn("avg_active_minutes_last7d", round(expr("avg(active_minutes)").over(window_spec)))

    features_live_path = f"{gold_base_path}/workout/features_live"
    features_df.select("user_id", "date", "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d") \
        .na.drop() \
        .write.mode("overwrite").partitionBy("date").parquet(features_live_path)
    print(f"  -> Live workout features saved to {features_live_path}")


if __name__ == "__main__":
    spark_session = get_spark_session()

    # Definiamo i percorsi S3a. Ora leggiamo i dati grezzi dal bucket 'bronze'.
    BRONZE_S3_PATH = 's3a://bronze'
    GOLD_S3_PATH = 's3a://gold'

    process_data(spark_session, BRONZE_S3_PATH, GOLD_S3_PATH)

    spark_session.stop()
    print("\n--- Esecuzione Job PySpark Completata ---")