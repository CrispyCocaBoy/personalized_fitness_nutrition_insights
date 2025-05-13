import time
import json
import random
from datetime import datetime, timezone
from paho.mqtt import client as mqtt_client

# Configurazione MQTT
broker = 'mqtt_broker'
port = 1883
topic = "wearables/bpm"
client_id = "bpm_producer"

# Connessione al database PostgreSQL (placeholder)
def get_user_ids():
    # Simulazione: 3 utenti
    return [1, 2, 3]

# Generatore dati BPM
def generate_heart_data(user_id):
    return {
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
        "user_id": str(user_id),
        "bpm": random.randint(60, 110)
    }

# Connessione MQTT
def connect_mqtt():
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("✅ MQTT Connected")
        else:
            print(f"❌ MQTT connection failed: {rc}")
    client = mqtt_client.Client(client_id=client_id,
                                callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

# Pubblica i dati BPM per tutti gli utenti
def publish(client, user_ids):
    while True:
        for user_id in user_ids:
            heart_data = generate_heart_data(user_id)
            msg = json.dumps(heart_data)
            result = client.publish(topic, msg, qos=1)
            status = result[0]
            if status == 0:
                print(f"📡 Sent {msg} to `{topic}`")
            else:
                print(f"⚠️ Failed to send message for user {user_id}")
            time.sleep(0.5)  # attesa tra utenti
        time.sleep(15)  # attesa tra cicli completi

# Run principale
def run():
    user_ids = get_user_ids()
    if not user_ids:
        print("❌ Nessun utente trovato nel database.")
        return
    client = connect_mqtt()
    client.loop_start()
    publish(client, user_ids)
    client.loop_stop()

if __name__ == '__main__':
    run()
