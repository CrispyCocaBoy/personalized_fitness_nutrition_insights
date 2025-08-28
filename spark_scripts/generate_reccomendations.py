# spark_scripts/generate_recommendations.py

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, row_number, collect_set, explode, vector_to_array, element_at
from pyspark.sql.window import Window
from pyspark.ml.pipeline import PipelineModel
import sys

# --- CONFIGURAZIONE GLOBALE ---
CONFIG = {
    "nutrition": {
        "user_cluster_map_path": "s3a://gold/models/nutrition/user_cluster_map/",
        "recommendations_path": "s3a://gold/nutrition/recommendations/",
        "classification_models_path": "s3a://gold/models/nutrition/classification_models/",
        "historical_data_path": "s3a://gold/nutrition/training_data/", # Per la candidate generation
        "db_table": "user_nutrition_rankings"
    },
    "workout": {
        "user_cluster_map_path": "s3a://gold/models/workout/user_cluster_map/",
        "recommendations_path": "s3a://gold/workout/recommendations/",
        "classification_models_path": "s3a://gold/models/workout/classification_models/",
        "historical_data_path": "s3a://gold/workout/training_data/",
        "db_table": "user_workout_rankings"
    }
}
K_CLUSTERS = 4
TOP_N_RECS = 10 # Quante raccomandazioni salvare per utente

def get_spark_session():
    """Configura e restituisce una sessione Spark."""
    return SparkSession.builder.appName("RecommendationPipeline").getOrCreate()

def main(spark, domain):
    print(f"\n--- AVVIO GENERAZIONE RACCOMANDAZIONI: {domain.upper()} ---")
    
    config = CONFIG[domain]

    # --- FASE 1: CANDIDATE GENERATION (ADDIO CROSSJOIN!) ---
    print("Fase 1: Generazione dei candidati per cluster...")
    
    # 1a. Carichiamo i dati storici e la mappa dei cluster
    historical_df = spark.read.parquet(config['historical_data_path'])
    user_cluster_map_df = spark.read.parquet(config['user_cluster_map_path'])
    
    # 1b. Troviamo le raccomandazioni popolari (feedback positivo) per ogni cluster
    popular_recs_per_cluster_df = historical_df.filter(col("feedback") == "positive") \
        .join(user_cluster_map_df, "user_id") \
        .groupBy("cluster_id") \
        .agg(collect_set("id_recommendation").alias("candidate_recs"))
    
    print("Raccomandazioni popolari per cluster:")
    popular_recs_per_cluster_df.show(truncate=False)

    # --- FASE 2: PREPARAZIONE INPUT PER INFERENZA ---
    print("\nFase 2: Preparazione input per inferenza...")
    
    # 2a. Carichiamo gli utenti "live" (tutti gli utenti per cui abbiamo un cluster)
    live_users_df = user_cluster_map_df
    
    # 2b. Uniamo ogni utente con la lista di candidati del suo cluster
    # e con il catalogo completo per avere le feature delle raccomandazioni
    recommendations_catalog_df = spark.read.parquet(config['recommendations_path'])
    
    inference_input_df = live_users_df.join(popular_recs_per_cluster_df, "cluster_id") \
        .withColumn("id_recommendation", explode(col("candidate_recs"))) \
        .join(recommendations_catalog_df, "id_recommendation")
        
    print(f"Numero di coppie (utente, raccomandazione) da valutare: {inference_input_df.count()}")

    # --- FASE 3: INFERENZA CON MODELLI PER CLUSTER ---
    print("\nFase 3: Esecuzione inferenza...")
    all_predictions = []
    for i in range(K_CLUSTERS):
        print(f"  - Esecuzione predizioni per il Cluster {i}")
        
        # Filtriamo l'input per il cluster corrente
        cluster_inference_df = inference_input_df.filter(col("cluster_id") == i)
        
        if cluster_inference_df.rdd.isEmpty():
            print(f"    -> Nessun utente/candidato per il cluster {i}. Salto.")
            continue
            
        # Carichiamo il modello specifico per questo cluster
        model_path = f"{config['classification_models_path']}cluster_{i}"
        classification_model = PipelineModel.load(model_path)
        
        predictions = classification_model.transform(cluster_inference_df)
        all_predictions.append(predictions)

    if not all_predictions:
        print("Nessuna predizione generata. Lo script termina.")
        sys.exit(0)

    # --- FASE 4: RANKING E SALVATAGGIO ---
    print("\nFase 4: Ranking dei risultati e salvataggio...")

    # 4a. Uniamo le predizioni da tutti i cluster
    full_predictions_df = all_predictions[0]
    for df in all_predictions[1:]:
        full_predictions_df = full_predictions_df.unionByName(df)

    # 4b. Estrazione della probabilità di successo (classe 1) CON FUNZIONI NATIVE
    predictions_with_prob = full_predictions_df.withColumn(
        "success_prob",
        element_at(vector_to_array(col("probability")), 2) # Gli array in Spark SQL sono 1-indicizzati
    )
    
    # 4c. Ranking delle raccomandazioni per ogni utente
    window_spec = Window.partitionBy("user_id").orderBy(col("success_prob").desc())
    ranked_recommendations_df = predictions_with_prob.withColumn("rank", row_number().over(window_spec))
    
    top_recs_df = ranked_recommendations_df.filter(col("rank") <= TOP_N_RECS)
    
    # 4d. Preparazione del DataFrame finale per il database
    final_df_to_save = top_recs_df.select(
        "user_id",
        "id_recommendation",
        "title",
        "description",
        "success_prob",
        "rank",
        current_timestamp().alias("generated_at")
    )
    
    print(f"Classifica finale (Top {TOP_N_RECS}) per la tabella '{config['db_table']}':")
    final_df_to_save.show(20, truncate=False)
    
    # 4e. Salvataggio su CockroachDB
    # db_properties = {"user": "root", "password": "", "driver": "org.postgresql.Driver"}
    # db_url = "jdbc:postgresql://cockroachdb:26257/defaultdb?sslmode=disable"
    # final_df_to_save.write.jdbc(
    #     url=db_url,
    #     table=config['db_table'],
    #     mode="overwrite", 
    #     properties=db_properties
    # )
    # print(f"Salvataggio su DB completato.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, choices=["nutrition", "workout"], help="Il dominio da processare ('nutrition' o 'workout')")
    args = parser.parse_args()
    
    spark_session = get_spark_session()
    main(spark_session, args.domain)
    spark_session.stop()