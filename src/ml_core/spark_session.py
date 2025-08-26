from pyspark.sql import SparkSession

def get_spark_session():
    """
    Inizializza e restituisce una SparkSession configurata per connettersi
    a MinIO e al cluster Spark.
    """
    return (
        SparkSession.builder
        .appName("FitnessRecommender")
        .master("spark://spark-master:7077")
        
        # La riga .config("spark.jars", ...) è stata RIMOSSA perché ora è gestita
        # da --packages nel comando spark-submit.
        
        # Configurazioni per MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        .getOrCreate()
    )
