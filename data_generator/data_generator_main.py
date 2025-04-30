# This rappresent the sensor that send data to the server

import time
import json
import random
from datetime import datetime, timezone
from paho.mqtt import client as mqtt_client

# Creazione del dato casuale (Da migliorare nettamente)
def generate_heart_data(sensor_id="sensor_001"):
    return {
        "t": datetime.now(timezone.utc).isoformat(),
        "id": sensor_id,
        "hr": random.randint(60, 100)  # battito cardiaco realistico
    }
#print(generate_heart_data(sensor_id="sensor_001"))

# Configurazione MQTT
broker = 'localhost'
port = 1883
topic = "sensors/heart"
client_id = "heart_sensor"
# username = 'emqx'
# password = 'public'

# Creazione del client
def connect_mqtt():
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id=client_id,
                                callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

# Definzione funzione publish
def publish(client):
    heart_data = generate_heart_data()
    msg = json.dumps(heart_data)  #Inserire il messaggio da mandare, in questo caso la lista
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)
    client.loop_stop()

if __name__ == '__main__':
    run()
