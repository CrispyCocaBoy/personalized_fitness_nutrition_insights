# app.py — Meals, Activities & Metrics Gateway con DuckDB + Delta (facts + daily)
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

# =====================================================
# Config da ambiente
# =====================================================
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION     = os.getenv("MINIO_REGION", "us-east-1")

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "broker_kafka:9092")
MEALS_TOPIC         = os.getenv("MEALS_TOPIC", "meals")
ACTIVITIES_TOPIC    = os.getenv("ACTIVITIES_TOPIC", "activities")
RECENT_BUFFER_SIZE  = int(os.getenv("RECENT_BUFFER_SIZE", "200"))

S3_BUCKET_GOLD      = "gold"
MEALS_FACT_PATH       = f"s3://{S3_BUCKET_GOLD}/meals_fact"
MEALS_DAILY_PATH      = f"s3://{S3_BUCKET_GOLD}/meals_daily"
METRICS_FACT_PATH     = f"s3://{S3_BUCKET_GOLD}/metrics_fact"
METRICS_DAILY_PATH    = f"s3://{S3_BUCKET_GOLD}/metrics_daily"
ACTIVITIES_FACT_PATH  = f"s3://{S3_BUCKET_GOLD}/activities_fact"
ACTIVITIES_DAILY_PATH = f"s3://{S3_BUCKET_GOLD}/activities_daily"

# =====================================================
# Kafka Producer
# =====================================================
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# =====================================================
# FastAPI + CORS
# =====================================================
app = FastAPI(title="Meals, Activities & Metrics Gateway", version="1.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================
# Pydantic models
# =====================================================
class MealIn(BaseModel):
    meal_id: str
    user_id: str
    meal_name: str = Field(..., min_length=1, max_length=200)
    kcal: int = 0
    carbs_g: int = 0
    protein_g: int = 0
    fat_g: int = 0
    timestamp: str
    notes: Optional[str] = ""

class MealAck(BaseModel):
    meal_id: str
    status: str = "queued"
    topic: str = MEALS_TOPIC

class ActivityIn(BaseModel):
    activity_event_id: str
    user_id: str
    activity_id: int
    activity_name: str = Field(..., min_length=1, max_length=200)
    start_ts: str
    end_ts: str
    notes: Optional[str] = ""

class ActivityAck(BaseModel):
    activity_event_id: str
    status: str = "queued"
    topic: str = ACTIVITIES_TOPIC

# =====================================================
# Buffer in-memory (fallback letture)
# =====================================================
RECENT_MEALS: deque = deque(maxlen=RECENT_BUFFER_SIZE)
RECENT_ACTIVITIES: deque = deque(maxlen=RECENT_BUFFER_SIZE)
RECENT_LOCK = Lock()

# =====================================================
# Helpers comuni
# =====================================================
def _delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}")

def _ensure_iso(ts: str) -> str:
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

def _normalize_endpoint(ep: str) -> tuple[str, bool]:
    ep = ep.strip()
    use_ssl = ep.startswith("https://")
    if ep.startswith("http://"):
        ep = ep[len("http://"):]
    elif ep.startswith("https://"):
        ep = ep[len("https://"):]
    return ep, use_ssl

SAFE_USER_RE     = re.compile(r"^[A-Za-z0-9_\-]+$")
SAFE_MEAL_ID_RE  = re.compile(r"^[A-Za-z0-9_\-:.]+$")
SAFE_EVENT_ID_RE = re.compile(r"^[A-Za-z0-9_\-:.]+$")
DATE_RE          = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def _validate_user(user_id: str):
    if not SAFE_USER_RE.match(user_id):
        raise HTTPException(status_code=400, detail="user_id non valido")

def _validate_meal_id(meal_id: str):
    if not SAFE_MEAL_ID_RE.match(meal_id):
        raise HTTPException(status_code=400, detail="meal_id non valido")

def _validate_activity_event_id(activity_event_id: str):
    if not SAFE_EVENT_ID_RE.match(activity_event_id):
        raise HTTPException(status_code=400, detail="activity_event_id non valido")

def _validate_date(d: Optional[str]):
    if d and not DATE_RE.match(d):
        raise HTTPException(status_code=400, detail="Data non valida (usa YYYY-MM-DD)")

def _fetch_as_dicts(sql: str):
    with DUCK_LOCK:
        rows = DUCK_CONN.execute(sql).fetchall()
        colnames = [d[0] for d in DUCK_CONN.description]
    return [dict(zip(colnames, r)) for r in rows]

# =====================================================
# DuckDB init
# =====================================================
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

print("✅ DuckDB delta+httpfs pronto")

# =====================================================
# Healthcheck
# =====================================================
@app.get("/healthz")
def healthz():
    producer.poll(0)
    try:
        with DUCK_LOCK:
            DUCK_CONN.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}

# =====================================================
# MEALS
# =====================================================
@app.post("/api/meals", response_model=MealAck)
def enqueue_meal(meal: MealIn):
    payload = {
        **meal.dict(),
        "task": "add",
        "timestamp": _ensure_iso(meal.timestamp),
        "ingest_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "gateway-rest",
        "schema_version": "1.0",
    }
    try:
        producer.produce(MEALS_TOPIC, key=meal.user_id, value=json.dumps(payload).encode("utf-8"), callback=_delivery_report)
        producer.poll(0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kafka produce error: {e}")
    with RECENT_LOCK:
        RECENT_MEALS.appendleft(payload)
    return MealAck(meal_id=meal.meal_id)

@app.get("/meals/facts")
def list_meals_facts(user_id: str, start_date: Optional[str]=None, end_date: Optional[str]=None, limit: int=25):
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT meal_id, user_id, meal_name,
             COALESCE(kcal,0) kcal, COALESCE(carbs_g,0) carbs_g, COALESCE(protein_g,0) protein_g, COALESCE(fat_g,0) fat_g,
             COALESCE(notes,'') notes,
             strftime(event_ts, '%Y-%m-%dT%H:%M:%S') || 'Z' event_ts,
             event_date
      FROM delta_scan('{MEALS_FACT_PATH}')
      {where}
      ORDER BY event_ts DESC
      LIMIT {limit}
    """
    return _fetch_as_dicts(sql)

@app.get("/meals/daily")
def list_meals_daily(user_id: str, start_date: Optional[str]=None, end_date: Optional[str]=None, limit: int=30):
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT user_id, event_date,
             kcal_total, carbs_total_g, protein_total_g, fat_total_g,
             meals_count,
             strftime(last_meal_ts, '%Y-%m-%dT%H:%M:%S') || 'Z' last_meal_ts
      FROM delta_scan('{MEALS_DAILY_PATH}')
      {where}
      ORDER BY event_date DESC
      LIMIT {limit}
    """
    return _fetch_as_dicts(sql)

@app.delete("/api/meals/{meal_id}", response_model=MealAck)
def delete_meal(
    meal_id: str,
    user_id: str = Query(..., description="User owner dell'evento")
):
    _validate_user(user_id)
    _validate_meal_id(meal_id)

    payload = {
        "task": "delete",
        "meal_id": meal_id,
        "user_id": user_id,
        "ingest_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "gateway-rest",
        "schema_version": "1.0",
    }

    try:
        producer.produce(
            MEALS_TOPIC,
            key=user_id,
            value=json.dumps(payload).encode("utf-8"),
            callback=_delivery_report
        )
        producer.poll(0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kafka produce error: {e}")

    # opzionale: tieni traccia in memoria
    with RECENT_LOCK:
        RECENT_MEALS.appendleft(payload)

    return MealAck(meal_id=meal_id, status="queued_delete")

# =====================================================
# METRICS
# =====================================================
@app.get("/metrics/facts")
def list_metrics_facts(user_id: str, start_date: Optional[str]=None, end_date: Optional[str]=None, limit: int=3000):
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT user_id,
             strftime(window_start, '%Y-%m-%dT%H:%M:%S') || 'Z' window_start,
             strftime(window_end, '%Y-%m-%dT%H:%M:%S') || 'Z' window_end,
             hr_bpm, spo2, step_count, stress_level, calories, event_date
      FROM delta_scan('{METRICS_FACT_PATH}')
      {where}
      ORDER BY window_end DESC
      LIMIT {limit}
    """
    return _fetch_as_dicts(sql)

@app.get("/metrics/daily")
def list_metrics_daily(user_id: str, start_date: Optional[str]=None, end_date: Optional[str]=None, limit: int=90):
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT user_id, event_date,
             hr_bpm_avg, hr_bpm_min, hr_bpm_max, spo2_avg,
             steps_total, calories_total,
             stress_low_cnt, stress_medium_cnt, stress_high_cnt,
             dominant_stress, windows_count,
             strftime(last_window_end, '%Y-%m-%dT%H:%M:%S') || 'Z' last_window_end
      FROM delta_scan('{METRICS_DAILY_PATH}')
      {where}
      ORDER BY event_date DESC
      LIMIT {limit}
    """
    return _fetch_as_dicts(sql)

# =====================================================
# ACTIVITIES
# =====================================================
@app.post("/api/activities", response_model=ActivityAck)
def enqueue_activity(activity: ActivityIn):
    payload = {
        **activity.dict(),
        "task": "add",
        "start_ts": _ensure_iso(activity.start_ts),
        "end_ts": _ensure_iso(activity.end_ts),
        "ingest_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "gateway-rest",
        "schema_version": "1.0",
    }
    try:
        producer.produce(ACTIVITIES_TOPIC, key=activity.user_id, value=json.dumps(payload).encode("utf-8"), callback=_delivery_report)
        producer.poll(0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kafka produce error: {e}")
    with RECENT_LOCK:
        RECENT_ACTIVITIES.appendleft(payload)
    return ActivityAck(activity_event_id=activity.activity_event_id)

@app.get("/activities/facts")
def list_activities_facts(
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 250
):
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT
         activity_event_id,
         user_id,
         activity_id,
         activity_name,
         -- KPI per attività (arricchiti dal job)
         hr_bpm_avg,
         COALESCE(calories_total, 0)  AS calories_total,
         COALESCE(steps_total, 0)     AS steps_total,
         COALESCE(distance_m, 0.0)    AS distance_m,
         pace_m_per_min,
         -- Altri campi
         COALESCE(duration_min, 0.0)  AS duration_min,
         COALESCE(notes, '')          AS notes,
         strftime(start_ts, '%Y-%m-%dT%H:%M:%S') || 'Z' AS start_ts,
         strftime(end_ts,   '%Y-%m-%dT%H:%M:%S') || 'Z' AS end_ts,
         event_date
      FROM delta_scan('{ACTIVITIES_FACT_PATH}')
      {where}
      ORDER BY start_ts DESC
      LIMIT {limit}
    """
    return _fetch_as_dicts(sql)


@app.get("/activities/daily")
def list_activities_daily(
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 30
):
    where = _where_user_and_dates(user_id, start_date, end_date)
    sql = f"""
      SELECT
         user_id,
         event_date,
         COALESCE(duration_total_min, 0.0) AS duration_total_min,
         COALESCE(activities_count, 0)     AS activities_count,
         strftime(last_activity_end_ts, '%Y-%m-%dT%H:%M:%S') || 'Z' AS last_activity_end_ts
      FROM delta_scan('{ACTIVITIES_DAILY_PATH}')
      {where}
      ORDER BY event_date DESC
      LIMIT {limit}
    """
    return _fetch_as_dicts(sql)


@app.delete("/api/activities/{activity_event_id}", response_model=ActivityAck)
def delete_activity(
    activity_event_id: str,
    user_id: str = Query(..., description="User owner dell'evento")
):
    _validate_user(user_id)
    _validate_activity_event_id(activity_event_id)

    payload = {
        "task": "delete",
        "activity_event_id": activity_event_id,
        "user_id": user_id,
        "_ingest_note": "logical delete request",
        "_ingest_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "gateway-rest",
        "schema_version": "1.0",
    }

    try:
        producer.produce(
            ACTIVITIES_TOPIC,
            key=user_id,
            value=json.dumps(payload).encode("utf-8"),
            callback=_delivery_report
        )
        producer.poll(0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kafka produce error: {e}")

    # opzionale: tieni traccia in memoria
    with RECENT_LOCK:
        RECENT_ACTIVITIES.appendleft(payload)

    return ActivityAck(activity_event_id=activity_event_id, status="queued_delete")

# =====================================================
# Common WHERE builder
# =====================================================
def _where_user_and_dates(user_id: str, start_date: Optional[str], end_date: Optional[str]) -> str:
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
