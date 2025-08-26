from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, first
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.clustering import KMeans
from ml_core.spark_session import get_spark_session

# --- CONFIGURAZIONE NUTRITION ---
TRAINING_DATA_PATH = "s3a://gold/nutrition/training_data/" # CORRETTO: Punta alla cartella
MODEL_OUTPUT_PATH = "s3a://gold/models/nutrition/clustering_model/"
MAP_OUTPUT_PATH = "s3a://gold/models/nutrition/user_cluster_map/"
K_CLUSTERS = 4

# --- FEATURE LIST AGGIORNATA ---
# Aggiunto 'bmi' per coerenza e accuratezza del modello
NUTRITION_FEATURES = [
    "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
    "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
    "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
]

def main(spark):
    print("\n--- AVVIO CLUSTERING PIPELINE: NUTRITION ---")
    
    # 1. Caricamento dei dati di training completi
    training_df = spark.read.parquet(TRAINING_DATA_PATH)
    
    # 2. AGGREGRAZIONE DATI UTENTE (MODIFICA CHIAVE)
    # Invece di usare dropDuplicates, aggreghiamo i dati per creare un profilo medio
    # e rappresentativo per ogni utente.
    user_features_df = training_df.groupBy("user_id").agg(
        first("age").alias("age"),
        first("gender").alias("gender"),
        first("height").alias("height"),
        mean("avg_weight_last7d").alias("avg_weight_last7d"),
        mean("bmi").alias("bmi"), # Aggiunto BMI
        mean("calories_consumed_last_3_days_avg").alias("calories_consumed_last_3_days_avg"),
        mean("protein_intake_last_3_days_avg").alias("protein_intake_last_3_days_avg"),
        mean("carbs_intake_last_3_days_avg").alias("carbs_intake_last_3_days_avg"),
        mean("fat_intake_last_3_days_avg").alias("fat_intake_last_3_days_avg")
    ).dropna() # Rimuoviamo utenti con dati incompleti
    
    print("DataFrame aggregato per il clustering:")
    user_features_df.show(5, truncate=False)

    # 3. Definizione della Pipeline di Machine Learning
    gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed", handleInvalid="skip")
    assembler = VectorAssembler(inputCols=NUTRITION_FEATURES, outputCol="unscaled_features", handleInvalid="skip")
    scaler = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features")
    kmeans = KMeans(featuresCol="scaled_features", k=K_CLUSTERS, seed=42, predictionCol="cluster_id")
    
    pipeline = Pipeline(stages=[gender_indexer, assembler, scaler, kmeans])
    
    # 4. Addestramento e Salvataggio del Modello
    print("Fitting del modello di clustering...")
    clustering_model = pipeline.fit(user_features_df)
    
    print(f"Salvataggio modello in: {MODEL_OUTPUT_PATH}")
    clustering_model.write().overwrite().save(MODEL_OUTPUT_PATH)
    
    # 5. Creazione e Salvataggio della Mappa Utente -> Cluster
    user_cluster_map = clustering_model.transform(user_features_df).select("user_id", "cluster_id")
    print(f"Salvataggio mappa utente->cluster in: {MAP_OUTPUT_PATH}")
    user_cluster_map.write.mode("overwrite").parquet(MAP_OUTPUT_PATH)
    
    print("--- Clustering NUTRITION completato. ---")

if __name__ == "__main__":
    spark_session = get_spark_session()
    main(spark_session)
    spark_session.stop()