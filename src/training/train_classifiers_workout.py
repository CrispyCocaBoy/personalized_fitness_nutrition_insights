from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from ml_core.spark_session import get_spark_session

# --- CONFIGURAZIONE WORKOUT ---
TRAINING_DATA_PATH = "s3a://gold/workout/training_data/"
USER_CLUSTER_MAP_PATH = "s3a://gold/models/workout/user_cluster_map/"
MODEL_OUTPUT_PATH_BASE = "s3a://gold/models/workout/classification_models/"

# FEATURE usate per la classificazione (devono essere coerenti con il clustering)
WORKOUT_FEATURES = [
    "age", "gender_indexed", "height", "avg_weight_last7d", "bmi", # <--- AGGIUNTO 'bmi'
    "avg_steps_last7d", "avg_bpm_last7d", "avg_active_minutes_last7d"
]
LABEL_COLUMN = "feedback"

def main(spark):
    print("\n--- AVVIO PIPELINE DI ADDESTRAMENTO CLASSIFICATORI: WORKOUT ---")

    # 1. Carica i dati di training e la mappa dei cluster
    training_df = spark.read.parquet(TRAINING_DATA_PATH)
    user_cluster_map = spark.read.parquet(USER_CLUSTER_MAP_PATH)

    # 2. Unisci i dati di training con i cluster assegnati a ogni utente
    data_with_clusters = training_df.join(user_cluster_map, "user_id")
    print("Dati di training uniti con la mappa dei cluster:")
    data_with_clusters.show()

    # 3. Ottieni la lista dei cluster unici per cui addestrare un modello
    cluster_ids = [row.cluster_id for row in data_with_clusters.select("cluster_id").distinct().collect()]
    print(f"Trovati i seguenti cluster per l'addestramento: {cluster_ids}\n")

    # 4. Itera su ogni cluster e addestra un classificatore specifico
    for cluster_id in cluster_ids:
        print(f"Addestramento del classificatore per il cluster {cluster_id}...")
        
        # Filtra i dati solo per il cluster corrente
        cluster_data = data_with_clusters.filter(col("cluster_id") == cluster_id)

        if cluster_data.count() == 0:
            print(f"  -> ATTENZIONE: Nessun dato per il cluster {cluster_id}. Salto l'addestramento.")
            continue

        # Definizione della pipeline di pre-processing e classificazione
        gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed", handleInvalid="skip")
        assembler = VectorAssembler(inputCols=WORKOUT_FEATURES, outputCol="unscaled_features", handleInvalid="skip")
        scaler = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features")
        
        # Usiamo un RandomForestClassifier come esempio
        rf = RandomForestClassifier(featuresCol="scaled_features", labelCol=LABEL_COLUMN, seed=42)
        
        pipeline = Pipeline(stages=[gender_indexer, assembler, scaler, rf])

        # Addestra il modello
        model = pipeline.fit(cluster_data)

        # Salva il modello specifico per questo cluster
        model_output_path = f"{MODEL_OUTPUT_PATH_BASE}cluster_{cluster_id}"
        print(f"  -> Salvataggio modello in: {model_output_path}")
        model.write().overwrite().save(model_output_path)
        print(f"  -> Addestramento per il cluster {cluster_id} completato.\n")

    print("--- Addestramento di tutti i classificatori WORKOUT completato. ---")

if __name__ == "__main__":
    spark_session = get_spark_session()
    main(spark_session)
    spark_session.stop()
