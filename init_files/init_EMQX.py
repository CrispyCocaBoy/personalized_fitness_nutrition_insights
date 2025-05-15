import requests
import json

BASE_URL = "http://localhost:18083/api/v5"  # Porta della EMQX Dashboard
HEADERS = {
    "Content-Type": "application/json"
}

# 1. Crea la resource Kafka
def create_kafka_resource():
    print("🔧 Creo resource Kafka...")
    data = {
        "type": "kafka",
        "name": "kafka_bridge",
        "config": {
            "bootstrap.servers": "kafka:9092"
        }
    }
    res = requests.post(f"{BASE_URL}/resources", headers=HEADERS, json=data)
    if res.status_code == 200:
        print("✅ Kafka resource creata.")
    else:
        print(f"❌ Errore nella creazione della resource Kafka: {res.text}")
        res.raise_for_status()

# 2. Crea la regola per inoltrare da MQTT a Kafka
def create_rule():
    print("📜 Creo regola per inoltrare su Kafka...")
    data = {
        "id": "bpm_rule",
        "sql": 'SELECT payload FROM "wearables/bpm"',
        "actions": [
            {
                "name": "data_to_kafka:send",
                "params": {
                    "topic": "wearables.bpm",
                    "resource": "kafka_bridge"
                }
            }
        ]
    }
    res = requests.post(f"{BASE_URL}/rules", headers=HEADERS, json=data)
    if res.status_code == 200:
        print("✅ Regola creata.")
    else:
        print(f"❌ Errore nella creazione della regola: {res.text}")
        res.raise_for_status()

# Esegui tutto
if __name__ == "__main__":
    create_kafka_resource()
    create_rule()


