#!/usr/bin/env python3
import os, sys, json, time, signal
from confluent_kafka import Consumer, KafkaException
import redis

# =========================
# Config (override via env)
# =========================
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
KAFKA_TOPIC_STATUS = os.getenv("KAFKA_TOPIC_STATUS", "toggle_sensor")
KAFKA_GROUP        = os.getenv("KAFKA_GROUP", "sensor-status-sync")
AUTO_OFFSET_RESET  = os.getenv("AUTO_OFFSET_RESET", "earliest")  # "latest" in prod

REDIS_HOST       = os.getenv("REDIS_HOST", "redis")
REDIS_PORT       = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB_STATUS  = int(os.getenv("REDIS_DB_STATUS", "0"))
REDIS_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "")             # es. "dev:"

BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "100"))
POLL_TIMEOUT_S    = float(os.getenv("POLL_TIMEOUT_S", "0.5"))
TOPIC_WAIT_SECS   = int(os.getenv("TOPIC_WAIT_SECS", "120"))

# =========================
# Utils
# =========================
def log(msg, lvl="INFO"):
    colors = {"INFO":"\033[94m","OK":"\033[92m","WARN":"\033[93m","ERR":"\033[91m"}
    print(f"{colors.get(lvl,'')}[{lvl}] {time.strftime('%Y-%m-%d %H:%M:%S')} | {msg}\033[0m")
    sys.stdout.flush()

def to_bool01(value):
    if isinstance(value, bool):   return "1" if value else "0"
    if isinstance(value, (int,float)): return "1" if int(value)!=0 else "0"
    if isinstance(value, str):
        v = value.strip().lower()
        return "1" if v in ("1","true","t","yes","y","on") else "0"
    return "0"

# =========================
# Connections
# =========================
def connect_kafka():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "enable.auto.commit": False,        # commit manuale
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "allow.auto.create.topics": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    }
    while True:
        try:
            log(f"Connessione a Kafka @ {KAFKA_BOOTSTRAP}…")
            c = Consumer(conf)
            # attendo che il topic esista
            deadline = time.time() + TOPIC_WAIT_SECS
            while time.time() < deadline:
                md = c.list_topics(timeout=5)
                if KAFKA_TOPIC_STATUS in md.topics:
                    log(f"✅ Topic '{KAFKA_TOPIC_STATUS}' trovato", "OK")
                    c.subscribe([KAFKA_TOPIC_STATUS])
                    return c
                log(f"⏳ Topic '{KAFKA_TOPIC_STATUS}' non ancora disponibile, retry…", "WARN")
                time.sleep(3)
            raise KafkaException(f"Topic '{KAFKA_TOPIC_STATUS}' non disponibile entro {TOPIC_WAIT_SECS}s")
        except KafkaException as e:
            log(f"Errore Kafka: {e}. Riprovo in 5s…", "WARN")
            time.sleep(5)

def connect_redis():
    while True:
        try:
            log(f"Connessione a Redis {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_STATUS}…")
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_STATUS, decode_responses=True)
            r.ping()
            log("✅ Connesso a Redis", "OK")
            return r
        except redis.exceptions.ConnectionError as e:
            log(f"Errore Redis: {e}. Riprovo in 5s…", "WARN")
            time.sleep(5)

# =========================
# Processing
# =========================
def handle_event(data, pipe):
    """
    data: dict deserializzato dal valore Kafka (JSON)
    aggiorna la pipeline redis con SET/DEL in base all'envelope.
    """
    # envelope wrapped: {before?, after?}
    if isinstance(data, dict) and ("before" in data or "after" in data):
        before = data.get("before")
        after  = data.get("after")

        # heartbeat
        if not before and not after:
            return 0

        # INSERT
        if after and not before:
            sid = after.get("sensor_id")
            if sid is not None:
                active = to_bool01(after.get("active", False))
                pipe.set(f"{REDIS_KEY_PREFIX}{sid}", active)
                return 1
            return 0

        # DELETE
        if before and not after:
            sid = before.get("sensor_id")
            if sid is not None:
                pipe.delete(f"{REDIS_KEY_PREFIX}{sid}")
                return 1
            return 0

        # UPDATE
        if before and after:
            sid = after.get("sensor_id", before.get("sensor_id"))
            if sid is not None:
                active = to_bool01(after.get("active", before.get("active", False)))
                pipe.set(f"{REDIS_KEY_PREFIX}{sid}", active)
                return 1
            return 0

    # envelope row fallback
    if isinstance(data, dict):
        sid = data.get("sensor_id")
        if sid is None:
            return 0
        if "active" in data:
            pipe.set(f"{REDIS_KEY_PREFIX}{sid}", to_bool01(data.get("active")))
            return 1
        # altrimenti ignora (oppure pipe.delete se vuoi considerarlo delete)
    return 0

running = True
def handle_sigterm(signum, frame):
    global running
    log("⏹️  Stop richiesto, chiusura…", "WARN")
    running = False

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

def main():
    consumer = connect_kafka()
    r = connect_redis()
    pipe = r.pipeline(transaction=False)
    buffered = 0
    log("🚀 Sensor Status CDC → Redis avviato")

    try:
        while running:
            msg = consumer.poll(POLL_TIMEOUT_S)
            if msg is None:
                # flush opportunistico se c'è buffer
                if buffered:
                    try:
                        pipe.execute()
                        consumer.commit(asynchronous=False)
                        log(f"✅ Committato batch di {buffered}", "OK")
                        buffered = 0
                    except Exception as e:
                        log(f"❌ Errore batch: {e}", "ERR")
                        pipe = r.pipeline(transaction=False)
                        buffered = 0
                continue

            if msg.error():
                log(f"⚠️ Kafka err: {msg.error()}", "WARN")
                continue

            try:
                raw = msg.value().decode("utf-8")
                data = json.loads(raw)
            except Exception as e:
                log(f"❌ JSON decode err: {e}", "ERR")
                continue

            buffered += handle_event(data, pipe)

            if buffered >= BATCH_SIZE:
                try:
                    pipe.execute()
                    consumer.commit(asynchronous=False)
                    log(f"✅ Committato batch di {buffered}", "OK")
                    buffered = 0
                except Exception as e:
                    log(f"❌ Errore batch: {e}", "ERR")
                    pipe = r.pipeline(transaction=False)
                    buffered = 0

    finally:
        # flush finale
        if buffered:
            try:
                pipe.execute()
                consumer.commit(asynchronous=False)
                log(f"✅ Flush finale {buffered}", "OK")
            except Exception as e:
                log(f"❌ Errore flush finale: {e}", "ERR")
        consumer.close()
        log("Consumer chiuso.")

if __name__ == "__main__":
    main()
