from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
import time
from datetime import datetime

# ⚙️ Crea SparkSession con accesso a MinIO (via s3a)
spark = SparkSession.builder \
    .appName("SimulateRecommenderData") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Simula dati feedback (user_id, item_id, rating, timestamp)
users = list(range(1, 21))            # 20 utenti
items = list(range(1, 11))            # 10 pasti/allenamenti
ratings = []

for _ in range(200):
    user = random.choice(users)
    item = random.choice(items)
    rating = random.randint(1, 5)  # rating da 1 a 5
    timestamp = int(time.time()) - random.randint(0, 60*60*24*30)
    ratings.append((user, item, rating, timestamp))

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

ratings_df = spark.createDataFrame(ratings, schema)

# 📤 Scrivi su MinIO in formato Parquet
ratings_df.write.mode("overwrite").parquet("s3a://data-lake/gold/recommender/training_data")

print("✅ Dataset scritto con successo su MinIO.")

spark.stop()
