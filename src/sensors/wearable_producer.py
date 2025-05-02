import time
import json
import random
from datetime import datetime, timezone
from paho.mqtt import client as mqtt_client

# === Profili utente ===
USER_PROFILES = [
    {"user_id": "user_001", "age": 25, "weight": 70, "sex": "M"},
    {"user_id": "user_002", "age": 53, "weight": 63, "sex": "F"},
    {"user_id": "user_003", "age": 45, "weight": 90, "sex": "M"},
]

# === Stati possibili ===
ACTIVITY_STATES = ["resting", "walking", "running", "sleeping"]
SLEEP_STAGES = ["light", "deep", "REM", "awake"]

# === Stato attuale degli utenti ===
USER_STATE = {user["user_id"]: {"activity": "resting", "current_hr": random.randint(65, 75)} for user in USER_PROFILES}

# === Generatore di dati fisiologici ===
def generate_heart_rate(user_id, activity):
    current_hr = USER_STATE[user_id]["current_hr"]

    def get_time_of_day_modifier():
        hour = datetime.now().hour
        if 0 <= hour <= 5:
            return -10
        elif 6 <= hour <= 9:
            return 0
        elif 10 <= hour <= 17:
            return 5
        elif 18 <= hour <= 22:
            return 0
        else:
            return -5

    def get_activity_modifier(activity):
        if activity == "sleeping":
            return random.randint(-15, -5)
        elif activity == "resting":
            return random.randint(-5, 5)
        elif activity == "walking":
            return random.randint(10, 20)
        elif activity == "running":
            return random.randint(30, 50)
        return 0

    drift = random.randint(-3, 3)
    time_modifier = get_time_of_day_modifier()
    activity_modifier = get_activity_modifier(activity)
    new_hr = current_hr + drift + time_modifier + activity_modifier

    if random.random() < 0.05:
        new_hr += random.randint(10, 30)

    new_hr = max(40, min(new_hr, 160))
    USER_STATE[user_id]["current_hr"] = new_hr
    return new_hr

# === Calcolo delle calorie bruciate ===
def calculate_calories_burned(user_profile, activity, heart_rate, steps, duration_min=1/12):
    weight = user_profile["weight"]
    age = user_profile["age"]
    sex = user_profile["sex"]

    if activity == "sleeping":
        met = 0.9
    elif activity == "resting":
        met = 1.0
    elif activity == "walking":
        met = 3.5 if steps < 100 else 4.5
    elif activity == "running":
        met = 8.0 if steps < 160 else 10.0
    else:
        met = 1.0

    calories = met * weight * (duration_min / 60)
    hr_factor = heart_rate / 100
    calories *= (0.7 + 0.3 * hr_factor)

    if sex == "M":
        calories *= 1.1
    if age > 50:
        calories *= 0.9

    calories = max(0.1, min(calories, 20))
    return round(calories, 2)

# === Transizioni di attività ===
def transition_activity(current):
    hour = datetime.now().hour
    if 23 <= hour or hour <= 6:
        return random.choices(["sleeping", "resting"], weights=[0.8, 0.2])[0]
    elif 6 < hour <= 8:
        return random.choices(["walking", "resting"], weights=[0.6, 0.4])[0]
    elif 8 < hour <= 18:
        return random.choices(["resting", "walking", "running"], weights=[0.5, 0.4, 0.1])[0]
    else:
        return random.choices(["resting", "walking"], weights=[0.7, 0.3])[0]

# === Generatore di dati biometrici ===
def generate_heart_data(user_profile):
    user_id = user_profile["user_id"]
    weight = user_profile["weight"]
    age = user_profile["age"]
    sex = user_profile["sex"]
    activity = USER_STATE[user_id]["activity"]

    heart_rate = generate_heart_rate(user_id, activity)
    steps = 0
    if activity == "walking":
        steps = random.randint(80, 120)
    elif activity == "running":
        steps = random.randint(140, 180)

    sleep_stage = None
    if activity == "sleeping":
        sleep_stage = random.choices(SLEEP_STAGES, weights=[0.5, 0.2, 0.2, 0.1])[0]

    calories_burned = calculate_calories_burned(user_profile, activity, heart_rate, steps)
    next_activity = transition_activity(activity)
    USER_STATE[user_id]["activity"] = next_activity

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "device_id": f"device_{user_id}",
        "age": age,
        "weight": weight,
        "sex": sex,
        "activity_type": activity,
        "heart_rate": heart_rate,
        "steps_per_minute": steps,
        "sleep_stage": sleep_stage,
        "calories_burned": calories_burned
    }

# === Configurazione MQTT ===
broker = 'mqtt_broker'  # Modifica in base al tuo broker
port = 1883
topic = "sensors/datawearable"
client_id = "wearable_sensor"

# === Connessione MQTT ===
def connect_mqtt():
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(f"Failed to connect, return code {rc}")

    client = mqtt_client.Client(client_id=client_id, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

# === Pubblicazione dati ===
def publish(client, interval_sec=5):
    while True:
        for user in USER_PROFILES:
            heart_data = generate_heart_data(user)
            msg = json.dumps(heart_data)
            result = client.publish(topic, msg, qos=1)
            status = result[0]
            if status == 0:
                print(f"Send `{msg}` to topic `{topic}`")
            else:
                print(f"Failed to send message to topic {topic}")
        time.sleep(interval_sec)

# === Avvio ===
def run():
    client = connect_mqtt()
    client.loop_start()
    try:
        publish(client)
    except KeyboardInterrupt:
        print("Simulation stopped")
    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected from MQTT Broker")

if __name__ == '__main__':
    run()