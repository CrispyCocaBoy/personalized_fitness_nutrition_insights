import boto3
from botocore.exceptions import ClientError

# Configurazione MinIO
minio_config = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin"
}

s3 = boto3.client("s3", **minio_config)

# Lista dei bucket
buckets = [
    "bronze",
    "silver",
    "gold",
    "checkpoints"
]

def create_bucket(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"⚠️  Bucket '{bucket_name}' già esistente.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            s3.create_bucket(Bucket=bucket_name)
            print(f"✅ Creato bucket '{bucket_name}'")
        else:
            print(f"❌ Errore: {e}")

def create_all():
    for bucket in buckets:
        create_bucket(bucket)

if __name__ == "__main__":
    create_all()

