import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# ========================
# 1. Spark Session
# ========================
spark = SparkSession.builder \
    .appName("DeltaLakeLatencyMonitor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Configurazione MinIO (S3A)
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", "minioadmin")
hadoopConf.set("fs.s3a.secret.key", "minioadmin")
hadoopConf.set("fs.s3a.endpoint", "http://minio:9000")
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# ========================
# 2. Configurazione monitoraggio
# ========================

# Lista tabelle da monitorare (iniziamo con Bronze Layer)
tables_to_monitor = [
    "s3a://bronze/wearables.ppg.raw/",
    "s3a://bronze/wearables.accelerometer.raw/",
    "s3a://bronze/wearables.ceda.raw/",
]

# Intervallo di refresh in secondi
refresh_interval = 30

print("✅ Monitor di latenza avviato. Premere Ctrl+C per fermare.")

try:
    while True:
        for table in tables_to_monitor:
            try:
                df = spark.read.format("delta").load(table)

                # Calcolo latenza media e massima
                df_latency = df.selectExpr(
                    "max(latency_sec) as max_latency_sec",
                    "avg(latency_sec) as avg_latency_sec",
                    "count(*) as total_events"
                )

                result = df_latency.collect()[0]
                print(f"\n--- Tabella: {table} ---")
                print(f"Totale eventi: {result['total_events']}")
                print(f"Latenza media: {result['avg_latency_sec']:.2f} sec")
                print(f"Latenza massima: {result['max_latency_sec']:.2f} sec")

            except Exception as e:
                print(f"⚠️ Errore durante la lettura della tabella {table}: {e}")

        print(f"\n⏳ Attendo {refresh_interval} secondi per il prossimo aggiornamento...\n")
        time.sleep(refresh_interval)

except KeyboardInterrupt:
    print("🛑 Monitor di latenza terminato manualmente.")
