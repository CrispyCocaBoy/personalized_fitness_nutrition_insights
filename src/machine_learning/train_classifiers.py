# spark_scripts/train_classifiers.py

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import sys

# --- CONFIGURAZIONE GLOBALE ---
# Un unico dizionario per gestire entrambi i domini, rendendo il codice riutilizzabile
CONFIG = {
    "nutrition": {
        "training_data_path": "s3a://gold/nutrition/training_data/",
        "recommendations_path": "s3a://gold/nutrition/recommendations/",
        "user_cluster_map_path": "s3a://gold/models/nutrition/user_cluster_map/",
        "model_output_path": "s3a://gold/models/nutrition/classification_models/",
        # Feature dell'utente (dal clustering)
        "user_features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
            "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
        ],
        # Feature della raccomandazione (dal catalogo)
        "rec_features": ["recommendation_type_indexed"]
    },
    "workout": {
        "training_data_path": "s3a://gold/workout/training_data/",
        "recommendations_path": "s3a://gold/workout/recommendations/",
        "user_cluster_map_path": "s3a://gold/models/workout/user_cluster_map/",
        "model_output_path": "s3a://gold/models/workout/classification_models/",
        # Feature dell'utente
        "user_features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
        ],
        # Feature della raccomandazione
        "rec_features": ["type_indexed", "difficulty_indexed", "duration"]
    }
}
K_CLUSTERS = 4


def get_spark_session():
    """Configura e restituisce una sessione Spark."""
    return SparkSession.builder.appName("TrainingPipeline").getOrCreate()


def main(spark, domain):
    print(f"\n--- AVVIO TRAINING PIPELINE: {domain.upper()} ---")

    config = CONFIG[domain]

    # 1. Caricamento di tutti i dati necessari
    print("Caricamento dati...")
    training_data_df = spark.read.parquet(config['training_data_path'])
    user_cluster_map_df = spark.read.parquet(config['user_cluster_map_path'])
    recommendations_df = spark.read.parquet(config['recommendations_path'])

    # 2. Creazione del Master Training DataFrame con un JOIN strategico
    master_df = training_data_df.join(user_cluster_map_df, "user_id") \
        .join(recommendations_df, "id_recommendation")

    master_df = master_df.withColumn("label", when(col("feedback") == "positive", 1).otherwise(0))

    # 3. Ciclo di addestramento: un modello per ogni cluster
    for i in range(K_CLUSTERS):
        print(f"\n--- Addestramento Modello per Cluster {i} ---")

        cluster_df = master_df.filter(col("cluster_id") == i)

        if cluster_df.count() < 20:  # Soglia minima per addestrare
            print(f"Cluster {i} non ha dati di training a sufficienza. Salto.")
            continue

        # 4. Divisione in Training e Test Set
        train_df, test_df = cluster_df.randomSplit([0.8, 0.2], seed=42)
        print(f"Dimensioni dataset: Training={train_df.count()}, Test={test_df.count()}")

        # 5. Definizione della Pipeline di Machine Learning
        indexers = [StringIndexer(inputCol="gender", outputCol="gender_indexed", handleInvalid="skip")]
        if domain == "nutrition":
            indexers.append(StringIndexer(inputCol="recommendation_type", outputCol="recommendation_type_indexed",
                                          handleInvalid="skip"))
        else:  # workout
            indexers.extend([
                StringIndexer(inputCol="type", outputCol="type_indexed", handleInvalid="skip"),
                StringIndexer(inputCol="difficulty", outputCol="difficulty_indexed", handleInvalid="skip")
            ])

        # CORREZIONE CRITICA: L'assembler ora usa SIA le feature utente SIA quelle della raccomandazione
        feature_cols = config['user_features'] + config['rec_features']
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

        rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
        pipeline = Pipeline(stages=indexers + [assembler, rf])

        # 6. Addestramento e Valutazione
        print("Addestramento del modello sul training set...")
        model = pipeline.fit(train_df)

        print("Valutazione del modello sul test set...")
        predictions = model.transform(test_df)

        evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                                  metricName="areaUnderROC")
        auc = evaluator.evaluate(predictions)
        print(f"===> Performance Cluster {i} - Test AUC: {auc:.4f} <===")

        # 7. Addestramento finale su tutti i dati del cluster e salvataggio
        print("Ri-addestramento del modello su tutti i dati per massimizzare l'apprendimento...")
        final_model = pipeline.fit(cluster_df)

        model_path = f"{config['model_output_path']}cluster_{i}"
        print(f"Salvataggio modello finale in: {model_path}")
        final_model.write().overwrite().save(model_path)

    print(f"\n--- TRAINING PIPELINE {domain.upper()} COMPLETATA ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, choices=["nutrition", "workout"],
                        help="Il dominio da processare ('nutrition' o 'workout')")
    args = parser.parse_args()

    spark_session = get_spark_session()
    main(spark_session, args.domain)
    spark_session.stop()