from paho.mqtt import client as mqtt_client
import json
import time
import random
import logging

# MQTT configuration
broker = 'emqx1'  # Docker container name of EMQX in the same network
port = 1883       # MQTT TCP port (without TLS)
client_id = "people_simulator"
username = 'emqx'
password = 'public'
topic_prefix = "wearables"

# Reconnection strategy
FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT Broker")
    else:
        print(f"Failed to connect, return code {rc}")

# Callback when the client disconnects
def on_disconnect(client, userdata, rc):
    logging.info("Disconnected with result code: %s", rc)
    reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
    while reconnect_count < MAX_RECONNECT_COUNT:
        logging.info("Reconnecting in %d seconds...", reconnect_delay)
        time.sleep(reconnect_delay)
        try:
            client.reconnect()
            logging.info("Reconnected successfully")
            return
        except Exception as err:
            logging.error("%s. Reconnect failed. Retrying...", err)
        reconnect_delay = min(reconnect_delay * RECONNECT_RATE, MAX_RECONNECT_DELAY)
        reconnect_count += 1
    logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)

# Create and configure the MQTT client
def connect_mqtt():
    client = mqtt_client.Client(client_id=client_id, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(broker, port)
    return client

# Create a payload for a single metric
def generate_payload(metric, value):
    return {
        "user_id": 1,
        "timestamp": int(time.time()),
        "metric": metric,
        "value": value
    }

# Publish loop for sending multiple metrics every 15 seconds
def publish_loop(client):
    while True:
        measurements = {
            "bpm": random.randint(60, 120),
            "hr": random.randint(60, 120),
            "hrv": round(random.uniform(20.0, 100.0), 2),
            "spo2": random.randint(94, 100),
            "steps": random.randint(0, 50),
            "skin_temperature": round(random.uniform(32.0, 37.5), 1)
        }

        for metric, value in measurements.items():
            topic = f"{topic_prefix}/{metric}"
            payload = generate_payload(metric, value)
            client.publish(topic, json.dumps(payload), qos=0)
            print(f"Published to `{topic}`: {payload}")

        time.sleep(15)

# Entry point
def run():
    time.sleep(15)
    client = connect_mqtt()
    client.loop_start()
    publish_loop(client)
    client.loop_stop()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("User_sensor_on")
    run()
