from pyspark.sql.functions import col, lit, row_number, current_timestamp
from pyspark.sql.types import DoubleType
from pyspark.ml.pipeline import PipelineModel
from ml_core.spark_session import get_spark_session
from pyspark.sql.functions import udf
from pyspark.sql.window import Window 

def get_probability_udf():
    """
    Definisce una UDF per estrarre la probabilità di successo (classe 1) 
    dal vettore di probabilità generato dal classificatore.
    """
    return udf(lambda v: float(v[1]) if v else 0.0, DoubleType())

if __name__ == "__main__":
    spark = get_spark_session()
    extract_prob_udf = get_probability_udf()

    # --- Percorsi e Nomi Corretti per NUTRITION ---
    FEATURES_LIVE_PATH = "s3a://gold/nutrition/features_live/"
    USER_PROFILES_PATH = "s3a://gold/nutrition/user_profiles/" # Aggiunto per identificare gli utenti live
    RECOMMENDATIONS_CATALOG_PATH = "s3a://gold/nutrition/recommendations/"
    CLUSTERING_MODEL_PATH = "s3a://gold/models/nutrition/clustering_model/"
    CLASSIFICATION_MODELS_BASE_PATH = "s3a://gold/models/nutrition/classification_models/"
    DB_TABLE = "user_nutrition_rankings"

    print("\n--- AVVIO JOB DI RACCOMANDAZIONE: NUTRITION ---")

    # 1. Caricamento dati e modelli
    live_features_df = spark.read.parquet(FEATURES_LIVE_PATH)
    user_profiles_df = spark.read.parquet(USER_PROFILES_PATH)
    recommendations_catalog_df = spark.read.parquet(RECOMMENDATIONS_CATALOG_PATH)
    clustering_model = PipelineModel.load(CLUSTERING_MODEL_PATH)

    # 2. Logica di inferenza dinamica
    # Identifichiamo gli utenti "live" (es. Luca e Marco) leggendo i profili.
    # Questo rende lo script robusto e mirato solo agli utenti di interesse.
    live_user_ids = [row.user_id for row in user_profiles_df.select("user_id").collect()]
    inference_input_df = live_features_df.filter(col("user_id").isin(live_user_ids))
    
    print("Dati di input per l'inferenza (solo utenti live):")
    inference_input_df.show(truncate=False)

    # 3. Assegnazione cluster agli utenti live
    users_with_clusters_df = clustering_model.transform(inference_input_df)
    
    # 4. Cross join per creare tutte le possibili coppie utente-raccomandazione
    inference_df = users_with_clusters_df.crossJoin(recommendations_catalog_df)
    
    # NOTA: La colonna di predizione del cluster si chiama 'cluster_id', non 'prediction'
    cluster_ids = [row.cluster_id for row in users_with_clusters_df.select("cluster_id").distinct().collect()]

    # 5. Inferenza per ogni cluster
    all_predictions = []
    for cluster_id in cluster_ids:
        print(f"\n--- Generazione predizioni per il Cluster {cluster_id} ---")
        model_path = f"{CLASSIFICATION_MODELS_BASE_PATH}cluster_{cluster_id}"
        classification_model = PipelineModel.load(model_path)
        
        cluster_inference_df = inference_df.filter(col("cluster_id") == cluster_id)
        
        if not cluster_inference_df.rdd.isEmpty():
            # --- FIX CRITICO: RIMOZIONE COLONNE INTERMEDIE ---
            # Rimuoviamo le colonne create dalla pipeline di clustering per evitare
            # un errore di "colonna già esistente" quando applichiamo la pipeline di classificazione.
            cols_to_drop = ["gender_indexed", "unscaled_features", "scaled_features"]
            df_for_classification = cluster_inference_df.drop(*cols_to_drop)
            
            predictions = classification_model.transform(df_for_classification)
            all_predictions.append(predictions)
            print(f" -> Predizioni per il cluster {cluster_id} generate.")

    if not all_predictions:
        print("Nessuna predizione generata. Lo script termina.")
        spark.stop()
        exit()

    # 6. Unione dei risultati e ranking
    full_predictions_df = all_predictions[0]
    for df in all_predictions[1:]:
        full_predictions_df = full_predictions_df.unionByName(df)

    predictions_with_prob = full_predictions_df.withColumn("success_prob", extract_prob_udf(col("probability")))
    
    window_spec = Window.partitionBy("user_id").orderBy(col("success_prob").desc())
    ranked_recommendations_df = predictions_with_prob.withColumn("rank", row_number().over(window_spec))
    
    top_10_recs_df = ranked_recommendations_df.filter(col("rank") <= 10)
    
    final_df_to_save = top_10_recs_df.select(
        col("user_id"),
        col("id_recommendation"),
        col("title").alias("nutrition_plan"),
        col("description").alias("details"),
        col("success_prob"),
        col("rank"),
        current_timestamp().alias("generated_at")
    )
    
    print(f"\nClassifica finale per la tabella '{DB_TABLE}':")
    final_df_to_save.show(truncate=False)

    # 7. Salvataggio su CockroachDB
    db_properties = {"user": "root", "password": "", "driver": "org.postgresql.Driver"}
    db_url = "jdbc:postgresql://cockroachdb:26257/defaultdb?sslmode=disable"
    
    final_df_to_save.write.jdbc(
        url=db_url,
        table=DB_TABLE,
        mode="overwrite", 
        properties=db_properties
    )

    print(f"\nJob di raccomandazione per '{DB_TABLE}' completato con successo!")
    spark.stop()