from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col, explode # Aggiunto explode per le raccomandazioni top-N

# Inizializza la SparkSession con supporto per MinIO (S3a)
spark = SparkSession.builder \
    .appName("ALS Recommendation Predictor") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Carica il modello ALS precedentemente salvato
model_path = "s3a://data-lake/models/als_fitness_nutrition_optimized"
als_model = ALSModel.load(model_path)

print(f"Modello ALS caricato con successo da: {model_path}")

# --- SCENARI DI PREDIZIONE ---
# Puoi scegliere uno dei seguenti blocchi in base a ciò che devi predire:

# SCENARIO 1: Predizioni di rating per specifiche coppie utente-item (utile per valutare)
# Questo scenario predice il rating che un utente darebbe a un item specifico.
# Per dimostrazione, ricarichiamo una parte dei dati di training.
# In un caso reale, qui andrebbero nuovi dati di utenti e item per cui non si ha un rating.
df_full_data = spark.read.parquet("s3a://data-lake/gold/recommender/training_data")
df_full_data_processed = df_full_data.select(
    col("user_id").cast("int"),
    col("item_id").cast("int"),
    col("rating").cast("float") # La colonna rating non è strettamente necessaria per la predizione, ma serve per il cast
).na.drop()
# Simula un set di dati per cui fare predizioni (es. prendi un campione)
# Prende solo user_id e item_id, poiché il rating sarà la nostra predizione
df_to_predict_specific_pairs = df_full_data_processed.select("user_id", "item_id").limit(10)

print("\nPredizioni per specifiche coppie utente-item:")
predictions_specific = als_model.transform(df_to_predict_specific_pairs)
predictions_specific.show()

# SCENARIO 2: Generazione di raccomandazioni top-N per tutti gli utenti (o un sottoinsieme)
# Questo è il caso d'uso più comune per un sistema di raccomandazione:
# trovare i migliori N item che un utente non ha ancora interagito.
print("\nGenerazione di raccomandazioni top-N per tutti gli utenti:")
# `recommendForAllUsers` restituisce un DataFrame con `user_id` e una colonna `recommendations`
# che è un array di struct `(item_id, rating)`.
recommendations_top_n = als_model.recommendForAllUsers(10) # Ottiene le 10 migliori raccomandazioni per ogni utente

# Espandi l'array delle raccomandazioni in righe separate per facilitare l'analisi
recommendations_exploded = recommendations_top_n.withColumn(
    "recommendation", explode(col("recommendations"))
).select(
    col("user_id"),
    col("recommendation.item_id").alias("recommended_item_id"),
    col("recommendation.rating").alias("predicted_rating")
)
recommendations_exploded.show()


# --- SALVATAGGIO DELLE PREDIZIONI ---
# Scegli quale set di predizioni salvare (es. le raccomandazioni top-N sono spesso più utili)
# Per questo esempio, salveremo le raccomandazioni top-N

output_path = "s3a://data-lake/silver/recommendations/personalized_recommendations" # Percorso per salvare le raccomandazioni
recommendations_exploded.write.mode("overwrite").parquet(output_path)

print(f"Raccomandazioni top-N salvate con successo in: {output_path}")

spark.stop()
print("SparkSession terminata.")