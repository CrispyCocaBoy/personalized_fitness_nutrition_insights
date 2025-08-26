from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, first
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.clustering import KMeans
from ml_core.spark_session import get_spark_session

# --- CONFIGURAZIONE WORKOUT ---
TRAINING_DATA_PATH = "s3a://gold/workout/training_data/"
MODEL_OUTPUT_PATH = "s3a://gold/models/workout/clustering_model/"
MAP_OUTPUT_PATH = "s3a://gold/models/workout/user_cluster_map/"
K_CLUSTERS = 4
# FEATURE usate per il clustering
WORKOUT_FEATURES = [
    "age", "gender_indexed", "height", "avg_weight_last7d", "bmi", # <--- AGGIUNTO 'bmi'
    "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
]

def main(spark):
    print("\n--- AVVIO CLUSTERING PIPELINE: WORKOUT ---")
    
    # Leggi il dataset di training completo
    training_df = spark.read.parquet(TRAINING_DATA_PATH)
    
    # Aggrega le feature per utente per avere un'unica riga rappresentativa per il clustering.
    # Usiamo 'mean' per le metriche di attività e 'first' per i dati anagrafici che non cambiano.
    user_features_df = training_df.groupBy("user_id").agg(
        first("age").alias("age"),
        first("gender").alias("gender"),
        first("height").alias("height"),
        mean("avg_weight_last7d").alias("avg_weight_last7d"),
        mean("bmi").alias("bmi"), # <--- AGGIUNTO il calcolo della media per 'bmi'
        mean("avg_steps_last7d").alias("avg_steps_last7d"),
        mean("avg_bpm_last7d").alias("avg_bpm_last7d"),
        mean("avg_active_minutes_last7d").alias("avg_active_minutes_last7d")
    ).dropna() # Rimuovi righe con dati mancanti dopo l'aggregazione
    
    print("DataFrame di input per il clustering (dati aggregati):")
    user_features_df.show(truncate=False)

    # Definizione della pipeline di clustering
    gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed", handleInvalid="skip")
    
    # Il VectorAssembler usa le feature (incluso 'gender_indexed')
    assembler = VectorAssembler(inputCols=WORKOUT_FEATURES, outputCol="unscaled_features", handleInvalid="skip")
    
    # Scalatura delle feature, cruciale per KMeans
    scaler = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features")
    
    # Modello di clustering KMeans
    kmeans = KMeans(featuresCol="scaled_features", k=K_CLUSTERS, seed=42, predictionCol="cluster_id")
    
    # Costruzione della pipeline completa
    pipeline = Pipeline(stages=[gender_indexer, assembler, scaler, kmeans])
    
    print("Fitting del modello di clustering...")
    clustering_model = pipeline.fit(user_features_df)
    
    print(f"Salvataggio modello in: {MODEL_OUTPUT_PATH}")
    clustering_model.write().overwrite().save(MODEL_OUTPUT_PATH)
    
    # Applica il modello per ottenere la mappa utente-cluster e salvala
    user_cluster_map = clustering_model.transform(user_features_df).select("user_id", "cluster_id")
    print(f"Salvataggio mappa utente->cluster in: {MAP_OUTPUT_PATH}")
    user_cluster_map.write.mode("overwrite").parquet(MAP_OUTPUT_PATH)
    
    print("--- Clustering WORKOUT completato. ---")

if __name__ == "__main__":
    spark_session = get_spark_session()
    main(spark_session)
    spark_session.stop()
