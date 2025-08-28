from pyspark.sql.functions import col, lit, row_number, current_timestamp, to_date
from pyspark.sql.types import DoubleType
from pyspark.ml.pipeline import PipelineModel
from ml_core.spark_session import get_spark_session
from pyspark.sql.functions import udf
from pyspark.sql.window import Window 
from datetime import datetime, date

def get_probability_udf():
    """
    Definisce una UDF (User Defined Function) per estrarre la probabilità 
    dalla colonna 'probability' generata dai modelli di classificazione.
    La probabilità di successo è il secondo elemento del vettore.
    """
    return udf(lambda v: float(v[1]) if v else 0.0, DoubleType())

if __name__ == "__main__":
    # Inizializza la sessione Spark
    spark = get_spark_session()
    # Ottiene la UDF per l'estrazione della probabilità
    extract_prob_udf = get_probability_udf()

    # --- CONFIGURAZIONE ---
    FEATURES_LIVE_PATH = "s3a://gold/workout/features_live/"
    USER_PROFILES_PATH = "s3a://gold/workout/user_profiles/"
    RECOMMENDATIONS_CATALOG_PATH = "s3a://gold/workout/recommendations/" 
    CLUSTERING_MODEL_PATH = "s3a://gold/models/workout/clustering_model/"
    CLASSIFICATION_MODELS_BASE_PATH = "s3a://gold/models/workout/classification_models/"
    DB_TABLE = "user_workout_rankings"
    
    # --- LOGICA DI ESECUZIONE GIORNALIERA ---
    # Ottiene la data odierna per l'esecuzione. Verranno usati solo i dati precedenti a questa data.
    TODAY_DATE = date.today()

    # Caricamento dati e modelli
    live_features_df = spark.read.parquet(FEATURES_LIVE_PATH)
    user_profiles_df = spark.read.parquet(USER_PROFILES_PATH) # Contiene solo gli utenti live (es. Luca e Marco)
    recommendations_catalog_df = spark.read.parquet(RECOMMENDATIONS_CATALOG_PATH)
    clustering_model = PipelineModel.load(CLUSTERING_MODEL_PATH)

    # --- OTTIMIZZAZIONE 1: Filtra i dati fino a "ieri" e seleziona i più recenti ---
    print(f"Filtraggio dei dati di attività fino al giorno prima di {TODAY_DATE}...")
    # Converte la colonna 'date' (che è stringa) in un tipo data per il confronto
    live_features_df = live_features_df.withColumn("date_ts", to_date(col("date")))
    
    # Filtra per tenere solo i record precedenti alla data odierna
    yesterdays_data_df = live_features_df.filter(col("date_ts") < lit(TODAY_DATE))

    # Seleziona solo il record più recente per ogni utente tra i dati disponibili
    window_spec_date = Window.partitionBy("user_id").orderBy(col("date_ts").desc())
    latest_live_features_df = yesterdays_data_df.withColumn("row_num", row_number().over(window_spec_date)) \
                                                .filter(col("row_num") == 1) \
                                                .drop("row_num", "date_ts")
    
    # --- OTTIMIZZAZIONE 2: Lavora solo con gli utenti "live" ---
    # Unendo i profili live con le loro feature più recenti, otteniamo dinamicamente
    # il set di dati di inferenza solo per gli utenti di interesse.
    inference_input_df = latest_live_features_df.join(user_profiles_df, on="user_id", how="inner").dropna()
    
    print("\nDataFrame finale per l'inferenza (solo utenti live con dati più recenti):")
    inference_input_df.show(truncate=False)

    # Assegnazione cluster agli utenti
    users_with_clusters_df = clustering_model.transform(inference_input_df)
    
    # Cross join per creare tutte le possibili coppie utente-raccomandazione
    inference_df = users_with_clusters_df.crossJoin(recommendations_catalog_df)
    
    cluster_ids = [row.cluster_id for row in users_with_clusters_df.select("cluster_id").distinct().collect()]

    all_predictions = []
    for cluster_id in cluster_ids:
        print(f"\nGenerazione predizioni per il cluster {cluster_id}...")
        model_path = f"{CLASSIFICATION_MODELS_BASE_PATH}cluster_{cluster_id}"
        classification_model = PipelineModel.load(model_path)
        
        cluster_inference_df = inference_df.filter(col("cluster_id") == cluster_id)
        
        if not cluster_inference_df.rdd.isEmpty():
            # FIX: Rimuovi le colonne intermedie per evitare conflitti
            cols_to_drop = ["gender_indexed", "unscaled_features", "scaled_features"]
            df_for_classification = cluster_inference_df.drop(*cols_to_drop)
            
            predictions = classification_model.transform(df_for_classification)
            all_predictions.append(predictions)
            print(f" -> Predizioni per il cluster {cluster_id} generate.")

    if not all_predictions:
        print("Nessuna predizione generata. Lo script termina.")
        spark.stop()
        exit()

    # Unisce tutti i DataFrame di predizioni
    full_predictions_df = all_predictions[0]
    for df in all_predictions[1:]:
        full_predictions_df = full_predictions_df.unionByName(df)

    # Estrazione della probabilità e ranking
    predictions_with_prob = full_predictions_df.withColumn("success_prob", extract_prob_udf(col("probability")))
    
    window_spec = Window.partitionBy("user_id").orderBy(col("success_prob").desc())
    ranked_recommendations_df = predictions_with_prob.withColumn("rank", row_number().over(window_spec))
    
    top_10_recs_df = ranked_recommendations_df.filter(col("rank") <= 10)
    
    final_df_to_save = top_10_recs_df.select(
        col("user_id"),
        col("recommendation_id"),
        col("title").alias("workout_type"),
        col("description").alias("details"),
        col("success_prob"),
        col("rank"),
        current_timestamp().alias("generated_at")
    )
    
    print(f"\nClassifica finale per {DB_TABLE}:")
    final_df_to_save.show(truncate=False)

    # Salvataggio su DB CockroachDB
    db_properties = {"user": "root", "password": "", "driver": "org.postgresql.Driver"}
    db_url = "jdbc:postgresql://cockroachdb:26257/defaultdb?sslmode=disable"
    
    final_df_to_save.write.jdbc(
        url=db_url,
        table=DB_TABLE,
        mode="overwrite",
        properties=db_properties
    )

    print(f"\nJob di raccomandazione per {DB_TABLE} completato con successo!")
    spark.stop()
