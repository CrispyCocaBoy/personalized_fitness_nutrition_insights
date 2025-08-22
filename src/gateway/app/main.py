# app.py — Meals & Metrics Gateway con DuckDB + Delta (facts + daily)
import os
import json
import re
from datetime import datetime, timezone
from typing import Optional
from collections import deque
from threading import Lock

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from confluent_kafka import Producer
import duckdb

# =========================
# Config da ambiente
# =========================
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION     = os.getenv("MINIO_REGION", "us-east-1")

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
MEALS_TOPIC         = os.getenv("MEALS_TOPIC", "meals")
RECENT_BUFFER_SIZE  = int(os.getenv("RECENT_BUFFER_SIZE", "200"))

S3_BUCKET_GOLD      = "gold"
# Nuove tabelle consolidate (partitionBy user_id,event_date)
MEALS_FACT_PATH     = f"s3://{S3_BUCKET_GOLD}/meals_fact"
MEALS_DAILY_PATH    = f"s3://{S3_BUCKET_GOLD}/meals_daily"
METRICS_FACT_PATH   = f"s3://{S3_BUCKET_GOLD}/metrics_fact"
METRICS_DAILY_PATH  = f"s3://{S3_BUCKET_GOLD}/metrics_daily"

# =========================
# Kafka Producer
# =========================
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# =========================
# FastAPI + CORS
# =========================
app = FastAPI(title="Meals & Metrics Gateway", version="1.0.0")
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
    timestamp: str  # ISO8601 string (preferibilmente UTC "Z")
    notes: Optional[str] = ""

class MealAck(BaseModel):
    meal_id: str
    status: str = "queued"
    topic: str = MEALS_TOPIC

# =========================
# Buffer in-memory (fallback letture)
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

SAFE_USER_RE  = re.compile(r"^[A-Za-z0-9_\-]+$")
DATE_RE       = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def _validate_user(user_id: str):
    if not SAFE_USER_RE.match(user_id):
        raise HTTPException(status_code=400, detail="user_id non valido")

def _validate_date(d: Optional[str]):
    if d and not DATE_RE.match(d):
        raise HTTPException(status_code=400, detail="Data non valida (usa YYYY-MM-DD)")

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
# Endpoints — Ingest
# =========================
@app.post("/api/meals", response_model=MealAck)
def enqueue_meal(meal: MealIn):
    """Riceve un pasto dalla UI e lo manda su Kafka (ingest)."""
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

# =========================
# Helpers — Query builder
# =========================
def _where_user_and_dates(user_id: str, start_date: Optional[str], end_date: Optional[str]) -> str:
    """
    Costruisce la WHERE per partition pruning su (user_id,event_date).
    """
    _validate_user(user_id)
    _validate_date(start_date)
    _validate_date(end_date)

    where = [f"user_id = '{user_id}'"]
    if start_date and end_date:
        where.append(f"event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'")
    elif start_date:
        where.append(f"event_date >= DATE '{start_date}'")
    elif end_date:
        where.append(f"event_date <= DATE '{end_date}'")
    return "WHERE " + " AND ".join(where)

def _fetch_as_dicts(sql: str):
    with DUCK_LOCK:
        rows = DUCK_CONN.execute(sql).fetchall()
        colnames = [d[0] for d in DUCK_CONN.description]
    return [dict(zip(colnames, r)) for r in rows]

# =========================
# Endpoints — Meals (FACT & DAILY)
# =========================
@app.get("/meals/facts")
def list_meals_facts(
    user_id: str = Query(..., description="ID utente (partition pruning)"),
    start_date: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    end_date:   Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(25, ge=1, le=1000),
):
    """
    Legge dai FACT consolidati: s3://gold/gold_meals_fact
    Ritorna i pasti ordinati per event_ts DESC.
    """
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT meal_id, user_id, meal_name,
             COALESCE(kcal,0)      AS kcal,
             COALESCE(carbs_g,0)   AS carbs_g,
             COALESCE(protein_g,0) AS protein_g,
             COALESCE(fat_g,0)     AS fat_g,
             COALESCE(notes,'')    AS notes,
             -- normalizzo in ISO8601 Z
             strftime(event_ts, '%Y-%m-%dT%H:%M:%S') || 'Z' AS event_ts,
             event_date
      FROM delta_scan('{MEALS_FACT_PATH}')
      {where}
      ORDER BY event_ts DESC
      LIMIT {int(limit)}
    """
    try:
        return _fetch_as_dicts(sql)
    except Exception as e:
        print(f"[WARN] Meals facts read error: {e} — using recent buffer")
        with RECENT_LOCK:
            return list(RECENT_MEALS)  # fallback best-effort

@app.get("/meals/daily")
def list_meals_daily(
    user_id: str,
    start_date: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    end_date:   Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(30, ge=1, le=1000),
):
    """
    Legge gli aggregati giornalieri: s3://gold/gold_meals_daily
    """
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT user_id, event_date,
             kcal_total, carbs_total_g, protein_total_g, fat_total_g,
             meals_count,
             strftime(last_meal_ts, '%Y-%m-%dT%H:%M:%S') || 'Z' AS last_meal_ts
      FROM delta_scan('{MEALS_DAILY_PATH}')
      {where}
      ORDER BY event_date DESC
      LIMIT {int(limit)}
    """
    try:
        return _fetch_as_dicts(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delta meals_daily read error: {e}")

# (Compatibilità retro) — mantiene il vecchio endpoint che la UI poteva usare
@app.get("/api/meals")
def list_meals_legacy(user_id: str, limit: int = 25):
    """
    Legacy: prima leggeva da s3://gold/users/{user_id}/meals.
    Ora reindirizziamo logicamente ai FACT consolidati.
    """
    return list_meals_facts(user_id=user_id, limit=limit)

# =========================
# Endpoints — Metrics (FACT & DAILY)
# =========================
@app.get("/metrics/facts")
def list_metrics_facts(
    user_id: str,
    start_date: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    end_date:   Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(3000, ge=1, le=50000),
):
    """
    Restituisce le metriche (finestre 1') dai FACT consolidati:
    s3://gold/gold_metrics_fact
    """
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT
        user_id,
        strftime(window_start, '%Y-%m-%dT%H:%M:%S') || 'Z' AS window_start,
        strftime(window_end,   '%Y-%m-%dT%H:%M:%S') || 'Z' AS window_end,
        hr_bpm, spo2, step_count, stress_level, calories,
        event_date
      FROM delta_scan('{METRICS_FACT_PATH}')
      {where}
      ORDER BY window_end DESC
      LIMIT {int(limit)}
    """
    try:
        return _fetch_as_dicts(sql)
    except Exception as e:
        print(f"[metrics/facts] read error: {e}")
        return []

@app.get("/metrics/daily")
def list_metrics_daily(
    user_id: str,
    start_date: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    end_date:   Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(90, ge=1, le=1000),
):
    """
    Restituisce gli aggregati giornalieri delle metriche:
    s3://gold/gold_metrics_daily
    """
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT
        user_id, event_date,
        hr_bpm_avg, hr_bpm_min, hr_bpm_max, spo2_avg,
        steps_total, calories_total,
        stress_low_cnt, stress_medium_cnt, stress_high_cnt,
        dominant_stress, windows_count,
        strftime(last_window_end, '%Y-%m-%dT%H:%M:%S') || 'Z' AS last_window_end
      FROM delta_scan('{METRICS_DAILY_PATH}')
      {where}
      ORDER BY event_date DESC
      LIMIT {int(limit)}
    """
    try:
        return _fetch_as_dicts(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delta metrics_daily read error: {e}")

# =========================
# Healthcheck
# =========================
@app.get("/healthz")
def healthz():
    producer.poll(0)
    # rapido probe su DuckDB/Delta opzionale
    try:
        with DUCK_LOCK:
            DUCK_CONN.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}
