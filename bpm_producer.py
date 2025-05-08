import time
import json
import random
from datetime import datetime, timezone
from paho.mqtt import client as mqtt_client
from utility import database_connection as db

# Configurazione MQTT
broker = 'mqtt_broker'
port = 1883
topic = "sensors/heart"
client_id = "heart_sensor"

# Connessione al database PostgreSQL
def get_user_ids():
    try:
        conn = db.connect()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users")
        user_ids = [row[0] for row in cur.fetchall()]
        conn.close()
        return user_ids
    except Exception as e:
        print(f"Errore connessione DB: {e}")
        return []

# Generatore dati BPM
def generate_heart_data(user_id):
    return {
        "t": datetime.now(timezone.utc).isoformat(),
        "id": user_id,
        "hr": random.randint(60, 100)
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
    for user_id in user_ids:
        heart_data = generate_heart_data(user_id)
        msg = json.dumps(heart_data)
        result = client.publish(topic, msg, qos=1)
        status = result[0]
        if status == 0:
            print(f"📡 Sent {msg} to `{topic}`")
        else:
            print(f"⚠️ Failed to send message for user {user_id}")
        time.sleep(0.2)  # evita flooding

# Run principale
def run():
    print("ok")
    user_ids = get_user_ids()
    if not user_ids:
        print("❌ Nessun utente trovato nel database.")
        return
    client = connect_mqtt()
    client.loop_start()
    publish(client, user_ids)
    client.loop_stop()
    print("✅ Finished sending BPM data")

if __name__ == '__main__':
    print("ok")
    #run()

