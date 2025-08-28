# dags/fitness_pipeline_dag.py

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- CONFIGURAZIONI CONDIVISE ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
S3_JARS_IN_AIRFLOW = "/opt/spark/connectors/hadoop-aws-3.3.4.jar,/opt/spark/connectors/aws-java-sdk-bundle-1.12.262.jar"

# Creiamo un dizionario con le configurazioni comuni per non ripeterci
SPARK_COMMON_CONFIG = {
    "conn_id": "spark_default",
    "jars": S3_JARS_IN_AIRFLOW,
    "conf": {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.shuffle.partitions": "8",
        # Limiti di risorse per stabilità su macchine locali
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1g",
        "spark.executor.cores": "1"
    },
    "verbose": True,
}

# --- DEFINIZIONE DEL DAG ---
with DAG(
    dag_id="fitness_ml_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["fitness_app", "ml"],
) as dag:
    
    # --- STADIO 1: FEATURE ENGINEERING ---
    build_features = SparkSubmitOperator(
        task_id="build_features",
        application="/opt/spark/apps/build_features.py",
        **SPARK_COMMON_CONFIG
    )

    # --- STADIO 2: CLUSTERING ---
    run_clustering_nutrition = SparkSubmitOperator(
        task_id="run_clustering_nutrition",
        application="/opt/spark/apps/run_clustering.py",
        application_args=["--domain", "nutrition"],
        **SPARK_COMMON_CONFIG
    )
    
    run_clustering_workout = SparkSubmitOperator(
        task_id="run_clustering_workout",
        application="/opt/spark/apps/run_clustering.py",
        application_args=["--domain", "workout"],
        **SPARK_COMMON_CONFIG
    )

    # --- STADIO 3: ADDESTRAMENTO MODELLI ---
    train_classifiers_nutrition = SparkSubmitOperator(
        task_id="train_classifiers_nutrition",
        application="/opt/spark/apps/train_classifiers.py",
        application_args=["--domain", "nutrition"],
        **SPARK_COMMON_CONFIG
    )

    train_classifiers_workout = SparkSubmitOperator(
        task_id="train_classifiers_workout",
        application="/opt/spark/apps/train_classifiers.py",
        application_args=["--domain", "workout"],
        **SPARK_COMMON_CONFIG
    )

    # --- STADIO 4: GENERAZIONE RACCOMANDAZIONI ---
    generate_recommendations_nutrition = SparkSubmitOperator(
        task_id="generate_recommendations_nutrition",
        application="/opt/spark/apps/generate_recommendations.py",
        application_args=["--domain", "nutrition"],
        **SPARK_COMMON_CONFIG
    )
    
    generate_recommendations_workout = SparkSubmitOperator(
        task_id="generate_recommendations_workout",
        application="/opt/spark/apps/generate_recommendations.py",
        application_args=["--domain", "workout"],
        **SPARK_COMMON_CONFIG
    )

    # --- DEFINIZIONE DELLA SEQUENZA DI ESECUZIONE ---
    # Per non sovraccaricare il PC, eseguiamo tutto in una lunga catena sequenziale:
    # Prima il build, poi tutta la pipeline di nutrition, e infine tutta quella di workout.
    build_features >> run_clustering_nutrition >> train_classifiers_nutrition >> generate_recommendations_nutrition
    
    generate_recommendations_nutrition >> run_clustering_workout >> train_classifiers_workout >> generate_recommendations_workout