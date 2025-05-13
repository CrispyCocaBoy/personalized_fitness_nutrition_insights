import paho.mqtt.client as mqtt
import json, time, random

client = mqtt.Client()


connected = False
while not connected:
    try:
        client.connect("mqtt_broker", 1883, 60)
        connected = True
    except ConnectionRefusedError:
        print("Broker MQTT non ancora disponibile, retry tra 3 secondi...")
        time.sleep(3)

client.loop_start()

while True:
    payload = {
        "user_id": "1",
        "timestamp": int(time.time()),
        "bpm": random.randint(60, 120)
    }
    client.publish("wearables/bpm", json.dumps(payload), qos=0)
    print("Pubblicato:", payload)
    time.sleep(15)
