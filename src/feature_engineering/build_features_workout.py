import pandas as pd
import os
from datetime import datetime
from minio import Minio
from io import BytesIO

# --- CONFIGURAZIONE ---
RAW_DATA_PATH = 'data/raw'
# Percorsi per il salvataggio diretto su MinIO
S3_BUCKET = 'gold'
S3_PROCESSED_PATH = 'workout'
S3_MODELS_PATH = 'models/workout'

# --- FUNZIONI DI PREPARAZIONE DATI ---

def get_minio_client():
    """Restituisce un client MinIO configurato."""
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def upload_to_minio(client, bucket, object_name, data):
    """Carica un oggetto su MinIO."""
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    client.put_object(
        bucket,
        object_name,
        BytesIO(data),
        length=len(data),
        content_type="application/octet-stream"
    )
    print(f" -> File caricato in s3a://{bucket}/{object_name}")

def process_recommendations(input_file, client):
    """
    Processa il catalogo delle raccomandazioni e lo salva su MinIO.
    """
    print("Processing recommendations catalog...")
    df = pd.read_csv(input_file)
    df.rename(columns={'difficulty_level': 'difficulty', 'duration_minutes': 'duration'}, inplace=True)
    difficulty_map = {'Basso': 'Low', 'Medio': 'Medium', 'Alto': 'High'}
    df['difficulty'] = df['difficulty'].map(difficulty_map)
    
    parquet_data = df.to_parquet(index=False, engine='pyarrow', compression='snappy')
    
    object_name = f"{S3_PROCESSED_PATH}/recommendations/recommendations.parquet"
    upload_to_minio(client, S3_BUCKET, object_name, parquet_data)
    print(f" -> Recommendations saved to s3a://{S3_BUCKET}/{S3_PROCESSED_PATH}/recommendations/\n")

def create_training_data(user_feedback_file, client):
    """
    Crea il dataset di training unificato e lo salva su MinIO.
    """
    print("Creating unified historical training data...")
    df_training = pd.read_csv(user_feedback_file)
    
    if 'is_positive' in df_training.columns:
        df_training.rename(columns={'is_positive': 'feedback'}, inplace=True)
        print("  - Standardized 'is_positive' to 'feedback' for training data.")

    print("  - Calculating BMI for training data...")
    df_training['bmi'] = df_training['avg_weight_last7d'] / ((df_training['height'] / 100) ** 2)

    parquet_data = df_training.to_parquet(index=False, engine='pyarrow', compression='snappy')

    object_name = f"{S3_PROCESSED_PATH}/training_data/training_data.parquet"
    upload_to_minio(client, S3_BUCKET, object_name, parquet_data)
    print(f" -> Training data saved to s3a://{S3_BUCKET}/{S3_PROCESSED_PATH}/training_data/\n")

def extract_user_profiles(synthetic_input_file, real_input_file, client):
    """
    Estrae i profili degli utenti e li salva su MinIO per il job di inferenza.
    """
    print("Extracting and combining user profiles...")
    
    df_synthetic = pd.read_csv(synthetic_input_file)
    profile_cols = ['user_id', 'age', 'gender', 'height', 'avg_weight_last7d']
    existing_cols = [col for col in profile_cols if col in df_synthetic.columns]
    synthetic_profiles_df = df_synthetic[existing_cols].drop_duplicates(subset=['user_id'])
    print(f"  Found {len(synthetic_profiles_df)} synthetic profiles from CSV.")

    real_profiles_df = pd.read_csv(real_input_file)
    real_profiles_df.rename(columns={'weight': 'avg_weight_last7d'}, inplace=True)
    print(f"  Found {len(real_profiles_df)} real user profiles from CSV.")

    combined_profiles_df = pd.concat([synthetic_profiles_df, real_profiles_df], ignore_index=True)
    combined_profiles_df.drop_duplicates(subset=['user_id'], keep='last', inplace=True)
    
    print("  - Calculating BMI for user profiles...")
    combined_profiles_df['bmi'] = combined_profiles_df['avg_weight_last7d'] / ((combined_profiles_df['height'] / 100) ** 2)
    print(f"  Total combined profiles: {len(combined_profiles_df)}")

    parquet_data = combined_profiles_df.to_parquet(index=False, engine='pyarrow', compression='snappy')
    
    object_name = f"{S3_PROCESSED_PATH}/user_profiles/user_profiles.parquet"
    upload_to_minio(client, S3_BUCKET, object_name, parquet_data)
    print(f" -> User profiles saved to s3a://{S3_BUCKET}/{S3_PROCESSED_PATH}/user_profiles/\n")

def build_live_features(input_file, client):
    """
    Crea le feature live per l'inferenza e le salva su MinIO.
    """
    print("Building features for live users (Luca & Marco)...")
    df = pd.read_csv(input_file)
    print("  Shifting dates to simulate August-October period...")
    df['date'] = pd.to_datetime(df['date'])
    first_date_original = df['date'].min()
    new_start_date = datetime(2025, 8, 1)
    date_offset = new_start_date - first_date_original
    df['date'] = df['date'] + date_offset
    print(f"  Dates shifted by {date_offset.days} days. New date range: {df['date'].min().date()} to {df['date'].max().date()}")
    df = df.sort_values(by=['user_id', 'date'])
    metrics_to_average = {
        'total_steps': 'avg_steps_last7d', 'avg_bpm': 'avg_bpm_last7d',
        'active_minutes': 'avg_active_minutes_last7d', 'calories_burned': 'avg_calories_last7d',
        'avg_hrv': 'avg_hrv_last7d'
    }
    print("  Calculating 7-day rolling averages for:")
    for source_col, target_col in metrics_to_average.items():
        print(f"  - {source_col} -> {target_col}")
        df[target_col] = df.groupby('user_id')[source_col].shift(1).rolling(window=7, min_periods=1).mean()
    final_columns = ['user_id', 'date'] + list(metrics_to_average.values())
    features_df = df[final_columns].dropna()
    
    # --- FIX: Converti la colonna 'date' in stringa prima di salvare ---
    features_df['date'] = features_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S') # <--- RIGA AGGIUNTA
    print("  - Converted 'date' column to string format for Spark compatibility.")

    parquet_data = features_df.to_parquet(index=False, engine='pyarrow', compression='snappy')
    object_name = f"{S3_PROCESSED_PATH}/features_live/features_live.parquet"
    upload_to_minio(client, S3_BUCKET, object_name, parquet_data)
    print(f"\n -> Live features saved to s3a://{S3_BUCKET}/{S3_PROCESSED_PATH}/features_live/\n")


if __name__ == "__main__":
    minio_client = get_minio_client()
    
    process_recommendations(
        input_file=os.path.join(RAW_DATA_PATH, 'workout_recommendations.csv'),
        client=minio_client
    )
    
    create_training_data(
        user_feedback_file=os.path.join(RAW_DATA_PATH, 'user_feedback_workout.csv'),
        client=minio_client
    )
    
    build_live_features(
        input_file=os.path.join(RAW_DATA_PATH, 'daily_user_metrics_live.csv'),
        client=minio_client
    )
    
    extract_user_profiles(
        synthetic_input_file=os.path.join(RAW_DATA_PATH, 'user_feedback_workout.csv'),
        real_input_file=os.path.join(RAW_DATA_PATH, 'user_profiles_live.csv'),
        client=minio_client
    )
    
    print("Data preparation completed successfully! All files are in MinIO.")
