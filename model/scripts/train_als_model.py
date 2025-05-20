from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col

# Import per la Cross-Validation
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Inizializza la SparkSession con supporto per MinIO (S3a)
spark = SparkSession.builder \
    .appName("ALS Recommendation Trainer with CV") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Carica i dati dal bucket
df = spark.read.parquet("s3a://data-lake/gold/recommender/training_data")

# Cast obbligatori (Spark ALS accetta solo Int/Float)
df = df.select(
    col("user_id").cast("int"),
    col("item_id").cast("int"),
    col("rating").cast("float")
).na.drop()

# Suddividi in training e test
# Nota: La Cross-Validation userà K-folds sul TRAINING set.
# Il test set finale sarà usato per una valutazione finale del modello BEST_MODEL.
(training, test) = df.randomSplit([0.8, 0.2], seed=42) # Aggiungo seed per riproducibilità

# 🧠 Crea l'istanza dell'algoritmo ALS
als = ALS(
    userCol="user_id",
    itemCol="item_id",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)

# 🛠️ Definisci la griglia di iperparametri da testare
# 'rank' è la dimensione della matrice latente (numero di feature latenti)
# 'regParam' è il parametro di regolarizzazione (previene l'overfitting)
param_grid = ParamGridBuilder() \
    .addGrid(als.rank, [10, 20]) \
    .addGrid(als.regParam, [0.01, 0.1]) \
    .build()

# 📊 Crea l'evaluator (metricName="rmse" è già quello che usiamo)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

# 🔄 Crea il CrossValidator
# estimator: l'algoritmo da addestrare (ALS)
# estimatorParamMaps: la griglia di parametri
# evaluator: la metrica per valutare i modelli
# numFolds: numero di fold per la cross-validation (es. 3 o 5)
# seed: per la riproducibilità della suddivisione dei fold
cv = CrossValidator(
    estimator=als,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3, # Puoi aumentare a 5 per una valutazione più robusta
    seed=42
)

print("\nAvvio della Cross-Validation e Grid Search per trovare il miglior modello ALS...")
# 🚂 Allena il CrossValidator sui dati di training
# Questo addestrerà ALS più volte con diverse combinazioni di parametri
# e selezionerà la migliore in base al RMSE medio sui fold.
cv_model = cv.fit(training)

# 🏆 Ottieni il miglior modello ALS trovato dalla Cross-Validation
best_als_model = cv_model.bestModel
print(f"Miglior rank trovato: {best_als_model._java_obj.parent().getRank()}")
print(f"Miglior regParam trovato: {best_als_model._java_obj.parent().getRegParam()}")

# 🎯 Valuta il miglior modello sul set di test (dati completamente non visti durante la CV)
predictions = best_als_model.transform(test)
rmse = evaluator.evaluate(predictions)
print(f"\nRMSE finale del miglior modello sul set di test = {rmse}")

# 💾 Salva il miglior modello trovato
# Sovrascriveremo il modello precedente con quello ottimizzato
model_output_path = "s3a://data-lake/models/als_fitness_nutrition_optimized" # Salva con un nome diverso per non sovrascrivere il vecchio
best_als_model.write().overwrite().save(model_output_path)
print(f"Miglior modello ALS salvato con successo in: {model_output_path}")

spark.stop()
print("SparkSession terminata.")
