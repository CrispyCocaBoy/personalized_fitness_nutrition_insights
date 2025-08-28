# spark_scripts/run_clustering.py

import argparse
from pyspark.sql.functions import col, lit, current_date, date_sub, row_number, desc
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.clustering import KMeans
# Assumiamo che la sessione Spark venga passata correttamente da Airflow
from pyspark.sql import SparkSession

# --- CONFIGURAZIONE GLOBALE ---
# Usiamo un dizionario per gestire le configurazioni specifiche per dominio
CONFIG = {
    "nutrition": {
        "training_data_path": "s3a://gold/nutrition/training_data/",
        "model_output_path": "s3a://gold/models/nutrition/clustering_model/",
        "map_output_path": "s3a://gold/models/nutrition/user_cluster_map/",
        "features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
            "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
        ]
    },
    "workout": {
        "training_data_path": "s3a://gold/workout/training_data/",
        "model_output_path": "s3a://gold/models/workout/clustering_model/",
        "map_output_path": "s3a://gold/models/workout/user_cluster_map/",
        "features": [
            "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
            "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
        ]
    }
}
K_CLUSTERS = 4
RECENCY_DAYS = 90 # Quanti giorni indietro guardare per definire il profilo di un utente

def get_spark_session():
    """Configura e restituisce una sessione Spark."""
    return SparkSession.builder.appName("ClusteringPipeline").getOrCreate()

def main(spark, domain):
    print(f"\n--- AVVIO CLUSTERING PIPELINE: {domain.upper()} ---")
    
    # 1. Caricamento della configurazione specifica per il dominio
    config = CONFIG[domain]
    
    # 2. Caricamento dati e FILTRO TEMPORALE
    print(f"Caricamento dati da: {config['training_data_path']}")
    
    # Per riproducibilità, potremmo passare la data da Airflow.
    # Per ora, usiamo la data corrente del sistema.
    yesterday = date_sub(current_date(), 1)
    start_date = date_sub(yesterday, RECENCY_DAYS)

    full_df = spark.read.parquet(config['training_data_path'])
    
    # Filtriamo per prendere solo i dati recenti e fino a ieri
    training_df = full_df.filter(
        (col("date") >= start_date) & (col("date") <= yesterday)
    )
    print(f"Dati filtrati per il periodo da {RECENCY_DAYS} giorni fa fino a ieri.")

    # 3. AGGREGAZIONE INTELLIGENTE: PRENDERE L'ULTIMO STATO DELL'UTENTE
    # Usiamo una Window function per trovare l'ultimo record di ogni utente nel periodo selezionato.
    window_spec = Window.partitionBy("user_id").orderBy(col("date").desc())
    
    user_features_df = training_df.withColumn("rank", row_number().over(window_spec)) \
                                .filter(col("rank") == 1) \
                                .drop("rank", "date", "id_recommendation", "feedback", "noted_at") # Rimuoviamo colonne non necessarie
    
    print("DataFrame con l'ultimo stato utente per il clustering:")
    user_features_df.show(5, truncate=False)

    # 4. Definizione della Pipeline di Machine Learning
    gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed", handleInvalid="skip")
    assembler = VectorAssembler(inputCols=config['features'], outputCol="unscaled_features", handleInvalid="skip")
    scaler = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features")
    kmeans = KMeans(featuresCol="scaled_features", k=K_CLUSTERS, seed=42, predictionCol="cluster_id")
    
    pipeline = Pipeline(stages=[gender_indexer, assembler, scaler, kmeans])
    
    # 5. Addestramento e Salvataggio del Modello
    print("Fitting del modello di clustering...")
    clustering_model = pipeline.fit(user_features_df)
    
    print(f"Salvataggio modello in: {config['model_output_path']}")
    clustering_model.write().overwrite().save(config['model_output_path'])
    
    # 6. Creazione e Salvataggio della Mappa Utente -> Cluster
    user_cluster_map = clustering_model.transform(user_features_df).select("user_id", "cluster_id")
    print(f"Salvataggio mappa utente->cluster in: {config['map_output_path']}")
    user_cluster_map.write.mode("overwrite").parquet(config['map_output_path'])
    
    print(f"--- Clustering {domain.upper()} completato. ---")

if __name__ == "__main__":
    # Aggiungiamo un parser per accettare argomenti da riga di comando
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, choices=["nutrition", "workout"], help="Il dominio da processare ('nutrition' o 'workout')")
    args = parser.parse_args()
    
    spark_session = get_spark_session()
    main(spark_session, args.domain)
    spark_session.stop()