# build_features_nutrition.py
import pandas as pd
import os
from datetime import datetime
from minio import Minio
from io import BytesIO

# --- CONFIGURAZIONE ---
RAW_DATA_PATH = 'data/raw'
# Percorsi per il salvataggio diretto su MinIO
S3_BUCKET = 'gold'
S3_PROCESSED_PATH = 'nutrition'

# --- FUNZIONI HELPER PER MINIO ---

def get_minio_client():
    """Restituisce un client MinIO configurato."""
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def upload_to_minio(client, bucket, object_name, data):
    """Carica un DataFrame in formato Parquet su MinIO."""
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    
    parquet_data = data.to_parquet(index=False, engine='pyarrow', compression='snappy')
    
    client.put_object(
        bucket,
        object_name,
        BytesIO(parquet_data),
        length=len(parquet_data),
        content_type="application/octet-stream"
    )
    print(f" -> File caricato con successo in s3a://{bucket}/{object_name}")

# --- FUNZIONI DI PREPARAZIONE DATI ---

def process_recommendations(client):
    """
    Processa il catalogo delle raccomandazioni nutrizionali e lo salva su MinIO.
    """
    print("\n--- Processing Nutrition Recommendations Catalog ---")
    input_file = os.path.join(RAW_DATA_PATH, 'nutrition_recommendations.csv')
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"ERRORE: File non trovato: {input_file}")
        return

    df.rename(columns={'recommendation_id': 'id_recommendation'}, inplace=True)
    
    object_name = f"{S3_PROCESSED_PATH}/recommendations/recommendations.parquet"
    upload_to_minio(client, S3_BUCKET, object_name, df)

def process_feedback_and_features(client):
    """
    Processa i dati di feedback, calcola le feature (incluso BMI),
    dinamizza le date e genera i file Parquet per training, live features e profili utente.
    """
    print("\n--- Processing User Feedback & Feature Engineering ---")
    input_file = os.path.join(RAW_DATA_PATH, 'user_feedback_nutrition.csv')
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"ERRORE: File non trovato: {input_file}")
        return

    # 1. Correzione e Dinamizzazione delle Date
    print(" 1. Shifting dates to simulate August period...")
    df['noted_at'] = pd.to_datetime(df['noted_at'])
    first_date_original = df['noted_at'].min()
    new_start_date = datetime(2025, 8, 1)
    date_offset = new_start_date - first_date_original
    df['noted_at'] = df['noted_at'] + date_offset
    print(f"    -> Dates shifted by {date_offset.days} days. New range: {df['noted_at'].min().date()} to {df['noted_at'].max().date()}")

    # 2. Calcolo del BMI
    print(" 2. Calculating BMI for all records...")
    df['bmi'] = df['avg_weight_last7d'] / ((df['height'] / 100) ** 2)
    df['bmi'] = df['bmi'].round(2)

    # 3. Conversione della data in stringa per compatibilità con Spark
    print(" 3. Converting 'noted_at' to string for Spark compatibility...")
    df['noted_at'] = df['noted_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 4. Salvataggio di 'training_data.parquet'
    print("\n   --- Generating training_data.parquet ---")
    training_object_name = f"{S3_PROCESSED_PATH}/training_data/training_data.parquet"
    upload_to_minio(client, S3_BUCKET, training_object_name, df)

    # 5. Creazione e salvataggio di 'features_live.parquet'
    print("\n   --- Generating features_live.parquet ---")
    LIVE_USER_IDS = [1, 2] # Luca e Marco
    live_df = df[df['user_id'].isin(LIVE_USER_IDS)].copy()
    
    # Prendi solo lo stato più recente per l'inferenza
    features_live_df = live_df.sort_values('noted_at').drop_duplicates(subset=['user_id'], keep='last')
    
    features_columns = [
        'user_id', 'age', 'gender', 'height', 'avg_weight_last7d', 'bmi',
        'calories_consumed_last_3_days_avg', 'protein_intake_last_3_days_avg',
        'carbs_intake_last_3_days_avg', 'fat_intake_last_3_days_avg'
    ]
    features_live_df = features_live_df[features_columns]
    
    live_object_name = f"{S3_PROCESSED_PATH}/features_live/features_live.parquet"
    upload_to_minio(client, S3_BUCKET, live_object_name, features_live_df)

    # 6. Creazione e salvataggio di 'user_profiles.parquet'
    print("\n   --- Generating user_profiles.parquet ---")
    profile_columns = ['user_id', 'gender', 'age', 'height', 'avg_weight_last7d', 'bmi']
    
    # Assicuriamoci di prendere il profilo più aggiornato per ogni utente
    user_profiles_df = df.sort_values(by='noted_at', ascending=False).drop_duplicates(subset=['user_id'])
    user_profiles_df = user_profiles_df[profile_columns].reset_index(drop=True)

    profiles_object_name = f"{S3_PROCESSED_PATH}/user_profiles/user_profiles.parquet"
    upload_to_minio(client, S3_BUCKET, profiles_object_name, user_profiles_df)


if __name__ == "__main__":
    print("--- Inizio Fase 1: Preparazione Dati Nutrizionali ---")
    minio_client = get_minio_client()
    
    # Processa e carica il catalogo delle raccomandazioni
    process_recommendations(minio_client)
    
    # Processa i dati di feedback per creare tutti gli altri file necessari
    process_feedback_and_features(minio_client)
    
    print("\n--- Fase 1 (Nutrizione) Completata con Successo! ---")
    print("Tutti i file Parquet sono stati generati e caricati su MinIO nel bucket 'gold'.")