import json
import boto3
from datetime import datetime, timezone
from paho.mqtt import client as mqtt_client

# Configurazione MQTT
broker = 'localhost'
port = 1883
topic = "sensors/heart"
client_id = "heart_listener"

# Configurazione MinIO (S3 compatibile)
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

bucket = 'bronze'

# Funzione per salvare i dati su MinIO
def save_to_minio(data):
    now = datetime.now(timezone.utc)
    key = f'bronze/heart_data/{now.date()}/sensor_{data["id"]}_{now.timestamp()}.json'
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data).encode('utf-8'))
    print(f"Dato salvato su MinIO: {key}")

# Funzione richiamata quando arriva un messaggio MQTT
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        print(f"Ricevuto: {data}")
        save_to_minio(data)
    except Exception as e:
        print(f"Errore nel processamento del messaggio: {e}")

# Connessione MQTT
def connect_mqtt():
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connesso al broker MQTT")
            client.subscribe(topic)
        else:
            print("Connessione fallita", rc)

    client = mqtt_client.Client(client_id=client_id,
                                callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port)
    return client

# Avvio
def run():
    client = connect_mqtt()
    client.loop_forever()

if __name__ == '__main__':
    run()
