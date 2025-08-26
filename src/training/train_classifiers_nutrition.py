from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
# Rimuoviamo l'evaluator, dato che addestriamo il modello finale per la produzione
# from pyspark.ml.evaluation import BinaryClassificationEvaluator 
from ml_core.spark_session import get_spark_session

# --- CONFIGURAZIONE NUTRITION ---
TRAINING_DATA_PATH = "s3a://gold/nutrition/training_data/"
CLUSTER_MAP_PATH = "s3a://gold/models/nutrition/user_cluster_map/"
MODEL_OUTPUT_BASE_PATH = "s3a://gold/models/nutrition/classification_models/"

# --- FEATURE LIST CORRETTA E COERENTE CON IL CLUSTERING ---
# Rimosso 'id_recommendation' (è una causa, non una feature)
# Aggiunti 'gender_indexed' e 'bmi' per coerenza
FEATURE_COLS = [
    "age", "gender_indexed", "height", "avg_weight_last7d", "bmi",
    "calories_consumed_last_3_days_avg", "protein_intake_last_3_days_avg",
    "carbs_intake_last_3_days_avg", "fat_intake_last_3_days_avg"
]
LABEL_COL = "feedback"

def main(spark):
    print("\n--- AVVIO TRAINING CLASSIFICATORI: NUTRITION ---")
    
    # 1. Carica i dati e la mappa dei cluster
    training_df = spark.read.parquet(TRAINING_DATA_PATH)
    cluster_map_df = spark.read.parquet(CLUSTER_MAP_PATH)
    
    # 2. Unisci i dati per sapere a quale cluster appartiene ogni record
    enriched_training_df = training_df.join(cluster_map_df, "user_id").cache()
    
    # 3. Ottieni la lista dei cluster unici per cui addestrare un modello
    cluster_ids = [row.cluster_id for row in enriched_training_df.select("cluster_id").distinct().collect()]
    print(f"Trovati i seguenti cluster per l'addestramento: {cluster_ids}\n")

    # 4. Itera su ogni cluster e addestra un classificatore specifico
    for cluster_id in cluster_ids:
        print(f"--- Processando Cluster {cluster_id} ---")
        cluster_data = enriched_training_df.filter(col("cluster_id") == cluster_id)
        
        if cluster_data.count() < 20: # Soglia minima per l'addestramento
            print(f"Skipping cluster {cluster_id} per dati insufficienti.")
            continue

        # --- PIPELINE DI PRE-PROCESSING E CLASSIFICAZIONE (MODIFICATA) ---
        # Deve essere identica a quella del clustering (+ il classificatore)
        gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed", handleInvalid="skip")
        assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="unscaled_features")
        scaler = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features")
        rf = RandomForestClassifier(featuresCol="scaled_features", labelCol=LABEL_COL, seed=42) # Usa le feature scalate
        
        # La pipeline ora contiene tutti gli step necessari
        pipeline = Pipeline(stages=[gender_indexer, assembler, scaler, rf])
        
        print(f"Addestramento del modello per il cluster {cluster_id}...")
        # Addestriamo sul 100% dei dati del cluster per il modello di produzione
        model = pipeline.fit(cluster_data)
        
        model_output_path = f"{MODEL_OUTPUT_BASE_PATH}cluster_{cluster_id}/"
        print(f"Salvataggio modello in: {model_output_path}")
        model.write().overwrite().save(model_output_path)
        print(f"Cluster {cluster_id} completato.\n")

    enriched_training_df.unpersist()
    print("\n--- Training Classificatori NUTRITION completato. ---")

if __name__ == "__main__":
    spark_session = get_spark_session()
    main(spark_session)
    spark_session.stop()