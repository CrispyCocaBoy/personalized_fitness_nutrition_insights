import json
import time
import sys
import signal
from confluent_kafka import Consumer, KafkaException
import redis

KAFKA_BOOTSTRAP = "broker_kafka:9092"
KAFKA_TOPIC = "users_changes"
KAFKA_GROUP = "redis-sync"

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 1

# ========= Utility Logging =========
def log(msg, level="INFO"):
    colors = {"INFO": "\033[94m", "OK": "\033[92m", "WARN": "\033[93m", "ERR": "\033[91m"}
    reset = "\033[0m"
    print(f"{colors.get(level, '')}[{level}] {time.strftime('%Y-%m-%d %H:%M:%S')} | {msg}{reset}")
    sys.stdout.flush()

# ========= Connessione Kafka con retry =========
def connect_kafka():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': KAFKA_GROUP,
        'auto.offset.reset': 'earliest'
    }

    while True:
        try:
            log(f"Connessione a Kafka su {KAFKA_BOOTSTRAP}...", "INFO")
            consumer = Consumer(consumer_conf)
            consumer.list_topics(timeout=5)  # Test connessione
            log("✅ Connesso a Kafka!", "OK")
            consumer.subscribe([KAFKA_TOPIC])
            log(f"Iscritto al topic: {KAFKA_TOPIC}", "OK")
            return consumer
        except KafkaException as e:
            log(f"Errore Kafka: {e}. Riprovo in 5s...", "WARN")
            time.sleep(5)

# ========= Connessione Redis con retry =========
def connect_redis():
    while True:
        try:
            log(f"Connessione a Redis su {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}...", "INFO")
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
            r.ping()
            log("✅ Connesso a Redis!", "OK")
            return r
        except redis.exceptions.ConnectionError as e:
            log(f"Errore Redis: {e}. Riprovo in 5s...", "WARN")
            time.sleep(5)

# ========= Graceful shutdown =========
running = True
def handle_sigterm(signum, frame):
    global running
    log("⏹️  Ricevuto segnale di stop, chiusura in corso...", "WARN")
    running = False

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

# ========= Main Loop =========
if __name__ == "__main__":
    consumer = connect_kafka()
    r = connect_redis()
    log("🚀 Kafka → Redis Sync Avviato", "INFO")

    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            log(f"⚠️ Errore Kafka: {msg.error()}", "WARN")
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            before, after = data.get('before'), data.get('after')

            # Nuova associazione sensore utente
            if after and not before:
                sensor_id = after['sensor_id']
                user_id = after['user_id']
                r.set(f"sensor:{sensor_id}", user_id)
                log(f"[SET] sensor:{sensor_id} -> {user_id}", "OK")

            # Rimozione associazione
            elif before and not after:
                sensor_id = before['sensor_id']
                r.delete(f"sensor:{sensor_id}")
                log(f"[DEL] sensor:{sensor_id}", "WARN")

        except Exception as e:
            log(f"❌ Errore parsing msg: {e}", "ERR")

    consumer.close()
    log("Consumer chiuso correttamente.", "INFO")
