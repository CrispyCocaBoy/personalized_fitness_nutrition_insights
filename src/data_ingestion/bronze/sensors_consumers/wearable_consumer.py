# Consumer per i dati biometrici wearable
import json
import boto3
import time
from datetime import datetime, timezone
from paho.mqtt import client as mqtt_client
import traceback

# === CONFIGURAZIONE ===
broker = 'mqtt_broker'
port = 1883
topic = "sensors/datawearable"
client_id = "datawearable_listener"

minio_endpoint = 'http://minio:9000'
access_key = 'minioadmin'
secret_key = 'minioadmin'
bucket = 'bronze'

# === CLIENT MINIO ===
s3 = boto3.client('s3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name='us-east-1'
)

# === SALVA SU MINIO ===
def save_to_minio(data):
    now = datetime.now(timezone.utc)
    # Usa user_id e timestamp per la chiave, coerente con il nuovo formato
    key = f'bronze/wearable_data/{now.date()}/user_{data["user_id"]}_{now.timestamp()}.json'
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data).encode('utf-8'))
    print(f"✅ Salvato su MinIO: {key}")

# === CALLBACK SU MESSAGGIO MQTT ===
def on_message(client, userdata, msg):
    print("📥 Messaggio ricevuto da MQTT!")
    try:
        data = json.loads(msg.payload.decode())
        print(f"📦 Payload decodificato: {data}")
        # Verifica che il messaggio contenga i campi attesi
        required_fields = ["timestamp", "user_id", "device_id", "heart_rate", "activity_type"]
        if all(field in data for field in required_fields):
            save_to_minio(data)
        else:
            print("❌ Messaggio non valido: campi mancanti")
    except Exception as e:
        print("❌ Errore nel processamento del messaggio:")
        print(traceback.format_exc())

# === CONNESSIONE MQTT ===
def connect_mqtt():
    def on_connect(client, userdata, flags, rc, properties=None):
        print("🔌 Tentativo di connessione a MQTT broker...")
        if rc == 0:
            print("✅ Connesso con successo!")
            client.subscribe(topic, qos=1)
            print(f"📡 Iscritto al topic: {topic}")
        else:
            print(f"❌ Connessione fallita con codice: {rc}")

    client = mqtt_client.Client(client_id=client_id,
                                callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    # DEBUG MQTT
    client.on_log = lambda c, u, l, s: print(f"[MQTT LOG] {s}")

    client.connect(broker, port)
    return client

# === AVVIO CONSUMER ===
def run():
    print("🚀 Avvio datawearable_consumer...")
    try:
        client = connect_mqtt()
        client.loop_forever()
    except Exception as e:
        print("❌ Errore generale:")
        print(traceback.format_exc())
        while True:
            time.sleep(10)  # Evita che il container muoia subito

if __name__ == '__main__':
    run()

