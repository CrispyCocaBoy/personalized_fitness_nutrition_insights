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

BATCH_SIZE = 1


def log(msg, level="INFO"):
    colors = {"INFO": "\033[94m", "OK": "\033[92m", "WARN": "\033[93m", "ERR": "\033[91m"}
    reset = "\033[0m"
    print(f"{colors.get(level, '')}[{level}] {time.strftime('%Y-%m-%d %H:%M:%S')} | {msg}{reset}")
    sys.stdout.flush()


def connect_kafka():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': KAFKA_GROUP,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    }
    while True:
        try:
            log(f"Connessione a Kafka su {KAFKA_BOOTSTRAP}...", "INFO")
            consumer = Consumer(consumer_conf)
            consumer.list_topics(timeout=5)
            log("✅ Connesso a Kafka!", "OK")

            # Aspetta che il topic sia disponibile
            log(f"Controllo disponibilità topic '{KAFKA_TOPIC}'...", "INFO")
            topic_found = False
            for attempt in range(30):  # 30 tentativi = 2.5 minuti max
                topics = consumer.list_topics(timeout=5).topics
                if KAFKA_TOPIC in topics:
                    log(f"✅ Topic '{KAFKA_TOPIC}' trovato!", "OK")
                    topic_found = True
                    break
                log(f"⏳ Topic '{KAFKA_TOPIC}' non ancora disponibile, attesa {attempt + 1}/30...", "WARN")
                time.sleep(5)

            if not topic_found:
                raise KafkaException(f"Topic '{KAFKA_TOPIC}' non disponibile dopo 2.5 minuti")

            consumer.subscribe([KAFKA_TOPIC])
            return consumer
        except KafkaException as e:
            log(f"Errore Kafka: {e}. Riprovo in 5s...", "WARN")
            time.sleep(5)


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


running = True


def handle_sigterm(signum, frame):
    global running
    log("⏹️  Ricevuto segnale di stop, chiusura in corso...", "WARN")
    running = False


signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

if __name__ == "__main__":
    consumer = connect_kafka()
    r = connect_redis()
    log("🚀 Kafka → Redis Sync Avviato", "INFO")

    pipe = r.pipeline(transaction=False)
    count = 0

    while running:
        msg = consumer.poll(0.2)
        if msg is None:
            continue

        if msg.error():
            log(f"⚠️ Errore Kafka: {msg.error()}", "WARN")
            continue

        try:
            raw_data = msg.value().decode('utf-8')
            # Debug temporaneo - rimuovi dopo aver verificato
            log(f"🔍 Raw message: {raw_data}", "INFO")

            data = json.loads(raw_data)
            log(f"🔍 Parsed data: {data}", "INFO")

            # Gestione per envelope = 'wrapped' (raccomandato)
            if 'after' in data or 'before' in data:
                before, after = data.get('before'), data.get('after')

                # Heartbeat o messaggio vuoto
                if not before and not after:
                    continue

                # INSERT: solo 'after'
                if after and not before:
                    sensor_id = after['sensor_id']
                    user_id = after['user_id']
                    pipe.set(f"sensor:{sensor_id}", user_id)
                    log(f"[INSERT] sensor:{sensor_id} -> {user_id}", "OK")

                # DELETE: solo 'before'
                elif before and not after:
                    sensor_id = before['sensor_id']
                    pipe.delete(f"sensor:{sensor_id}")
                    log(f"[DELETE] sensor:{sensor_id}", "WARN")

                # UPDATE: sia 'before' che 'after'
                elif before and after:
                    old_sensor_id = before['sensor_id']
                    new_sensor_id = after['sensor_id']
                    new_user_id = after['user_id']

                    if old_sensor_id != new_sensor_id:
                        # Sensor ID è cambiato
                        pipe.delete(f"sensor:{old_sensor_id}")
                        pipe.set(f"sensor:{new_sensor_id}", new_user_id)
                        log(f"[UPDATE] sensor:{old_sensor_id} -> sensor:{new_sensor_id} = {new_user_id}", "OK")
                    else:
                        # Solo user_id è cambiato
                        pipe.set(f"sensor:{new_sensor_id}", new_user_id)
                        log(f"[UPDATE] sensor:{new_sensor_id} -> {new_user_id}", "OK")

            # Gestione per envelope = 'row' (fallback)
            else:
                # Con envelope = 'row' hai direttamente i dati della riga
                if 'sensor_id' in data and 'user_id' in data:
                    sensor_id = data['sensor_id']
                    user_id = data['user_id']
                    pipe.set(f"sensor:{sensor_id}", user_id)
                    log(f"[SET] sensor:{sensor_id} -> {user_id}", "OK")
                else:
                    log(f"⚠️ Messaggio sconosciuto: {data}", "WARN")
                    continue

            count += 1
            if count >= BATCH_SIZE:
                try:
                    pipe.execute()
                    consumer.commit(asynchronous=False)
                    log(f"✅ Batch di {count} operazioni committato", "OK")
                    count = 0
                except Exception as e:
                    log(f"❌ Errore Redis batch: {e}", "ERR")
                    # Reset pipeline in caso di errore
                    pipe = r.pipeline(transaction=False)
                    count = 0

        except json.JSONDecodeError as e:
            log(f"❌ Errore JSON parsing: {e}", "ERR")
        except Exception as e:
            log(f"❌ Errore generico: {e}", "ERR")

    # Flush finale
    if count > 0:
        try:
            pipe.execute()
            consumer.commit(asynchronous=False)
            log(f"✅ Flush finale: {count} operazioni", "OK")
        except Exception as e:
            log(f"❌ Errore flush finale: {e}", "ERR")

    consumer.close()
    log("Consumer chiuso correttamente.", "INFO")