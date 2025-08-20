# app.py — Meals Gateway con Delta Lake (fact + daily)

import os
import json
import re
from datetime import datetime, timezone
from typing import Optional
from collections import deque
from threading import Lock

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from confluent_kafka import Producer
import duckdb

# =========================
# Config da ambiente
# =========================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
MEALS_TOPIC = os.getenv("MEALS_TOPIC", "meals")
RECENT_BUFFER_SIZE = int(os.getenv("RECENT_BUFFER_SIZE", "200"))

# =========================
# Kafka Producer
# =========================
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# =========================
# FastAPI + CORS
# =========================
app = FastAPI(title="Meals Gateway", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # restringere in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Pydantic models
# =========================
class MealIn(BaseModel):
    meal_id: str
    user_id: str
    meal_name: str = Field(..., min_length=1, max_length=200)
    kcal: int = 0
    carbs_g: int = 0
    protein_g: int = 0
    fat_g: int = 0
    timestamp: str  # ISO8601 string
    notes: Optional[str] = ""

class MealAck(BaseModel):
    meal_id: str
    status: str = "queued"
    topic: str = MEALS_TOPIC

# =========================
# Buffer in-memory
# =========================
RECENT_MEALS: deque = deque(maxlen=RECENT_BUFFER_SIZE)
RECENT_LOCK = Lock()

# =========================
# Helpers
# =========================
def _delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}")

def _ensure_iso(ts: str) -> str:
    """Normalizza timestamp in ISO8601 UTC Z."""
    try:
        if ts.endswith("Z"):
            dt = datetime.fromisoformat(ts[:-1])
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _to_kafka_payload(m: MealIn) -> dict:
    return {
        "meal_id": m.meal_id,
        "user_id": m.user_id,
        "meal_name": m.meal_name,
        "kcal": m.kcal,
        "carbs_g": m.carbs_g,
        "protein_g": m.protein_g,
        "fat_g": m.fat_g,
        "timestamp": _ensure_iso(m.timestamp),
        "notes": m.notes or "",
        "ingest_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "gateway-rest",
        "schema_version": "1.0",
    }

def _normalize_endpoint(ep: str) -> tuple[str, bool]:
    """Ritorna (host:port, use_ssl)."""
    ep = ep.strip()
    use_ssl = ep.startswith("https://")
    if ep.startswith("http://"):
        ep = ep[len("http://"):]
    elif ep.startswith("https://"):
        ep = ep[len("https://"):]
    return ep, use_ssl

SAFE_USER_RE = re.compile(r"^[A-Za-z0-9_\-]+$")

# =========================
# DuckDB + delta extension
# =========================
DUCK_CONN = duckdb.connect(database=":memory:")
DUCK_LOCK = Lock()

DUCK_CONN.execute("INSTALL httpfs; LOAD httpfs;")
DUCK_CONN.execute("INSTALL delta;  LOAD delta;")

secret_name = "minio_secret"
endpoint_hostport, use_ssl = _normalize_endpoint(MINIO_ENDPOINT)
DUCK_CONN.execute(f"DROP SECRET IF EXISTS {secret_name}")
DUCK_CONN.execute(f"""
CREATE SECRET {secret_name} (
  TYPE S3,
  KEY_ID '{MINIO_ACCESS_KEY}',
  SECRET '{MINIO_SECRET_KEY}',
  REGION '{MINIO_REGION}',
  ENDPOINT '{endpoint_hostport}',
  URL_STYLE 'path',
  USE_SSL {'true' if use_ssl else 'false'}
)
""")

print("✅ DuckDB delta+httpfs pronto (secret S3 configurato)")

# =========================
# Endpoints
# =========================
@app.post("/api/meals", response_model=MealAck)
def enqueue_meal(meal: MealIn):
    """Riceve un pasto dalla UI e lo manda su Kafka."""
    payload = _to_kafka_payload(meal)
    try:
        producer.produce(
            topic=MEALS_TOPIC,
            key=meal.user_id,
            value=json.dumps(payload).encode("utf-8"),
            callback=_delivery_report,
        )
        producer.poll(0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kafka produce error: {e}")

    with RECENT_LOCK:
        RECENT_MEALS.appendleft(payload)
    return MealAck(meal_id=meal.meal_id)

@app.get("/api/meals")
def list_meals(user_id: str, limit: int = 25):
    """Legge i pasti (fact table)."""
    if not SAFE_USER_RE.match(user_id):
        raise HTTPException(status_code=400, detail="user_id non valido")

    user_meals_path = f"s3://gold/users/{user_id}/meals"
    limit = max(1, min(int(limit), 1000))

    probe_sql = f"SELECT * FROM delta_scan('{user_meals_path}') LIMIT 1"
    try:
        with DUCK_LOCK:
            probe_df = DUCK_CONN.execute(probe_sql).fetchdf()
        cols = set(probe_df.columns)
        ts_col = "event_ts" if "event_ts" in cols else ("ts" if "ts" in cols else None)
    except Exception:
        ts_col = None

    sql = f"""
      SELECT meal_id, user_id, meal_name,
             COALESCE(kcal,0)        AS kcal,
             COALESCE(carbs_g,0)     AS carbs_g,
             COALESCE(protein_g,0)   AS protein_g,
             COALESCE(fat_g,0)       AS fat_g,
             COALESCE(notes,'')      AS notes
             {", strftime(" + ts_col + ", '%Y-%m-%dT%H:%M:%S') || 'Z' AS timestamp" if ts_col else ""}
      FROM delta_scan('{user_meals_path}')
      {f"ORDER BY {ts_col} DESC" if ts_col else ""}
      LIMIT {limit}
    """

    try:
        with DUCK_LOCK:
            rows = DUCK_CONN.execute(sql).fetchall()
            colnames = [d[0] for d in DUCK_CONN.description]
        return [dict(zip(colnames, r)) for r in rows]
    except Exception as e:
        print(f"[WARN] Delta read fallback: {e}")
        with RECENT_LOCK:
            return list(RECENT_MEALS)

@app.get("/api/meals_daily")
def list_meals_daily(user_id: str, limit: int = 30):
    """Legge gli aggregati giornalieri (meals_daily)."""
    if not SAFE_USER_RE.match(user_id):
        raise HTTPException(status_code=400, detail="user_id non valido")

    user_daily_path = f"s3://gold/users/{user_id}/meals_daily"
    sql = f"""
      SELECT user_id, event_date,
             kcal_total, carbs_total_g, protein_total_g, fat_total_g,
             meals_count, last_meal_ts
      FROM delta_scan('{user_daily_path}')
      ORDER BY event_date DESC
      LIMIT {limit}
    """
    try:
        with DUCK_LOCK:
            rows = DUCK_CONN.execute(sql).fetchall()
            colnames = [d[0] for d in DUCK_CONN.description]
        return [dict(zip(colnames, r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delta daily read error: {e}")

@app.get("/healthz")
def healthz():
    producer.poll(0)
    return {"status": "ok"}
