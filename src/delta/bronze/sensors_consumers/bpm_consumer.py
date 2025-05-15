from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, LongType
import traceback
import time

# Schema del JSON che arriva da Kafka
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("bpm", IntegerType()) \
    .add("timestamp", LongType())

spark = None
query = None

try:
    # Inizializza SparkSession
    spark = SparkSession.builder \
        .appName("BPM Consumer") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✅ SparkSession inizializzata")

    # Lettura da Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "wearables.bpm") \
        .option("startingOffsets", "earliest") \
        .load()

    print("📥 Connessione a Kafka stabilita, topic: wearables.bpm")

    # Parsing JSON
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    print("🧠 Schema applicato, avvio del writeStream...")


    # Scrittura su Delta Lake (MinIO)
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://bronze/bpm/_checkpoints") \
        .start("s3a://bronze/bpm/")

    query.awaitTermination()

except Exception as e:
    print("❌ Errore durante l'esecuzione del consumer:")
    traceback.print_exc()

finally:
    print("🛑 Shutdown del job Spark")
    if query:
        try:
            query.stop()
        except:
            pass
    if spark:
        try:
            spark.stop()
        except:
            pass
