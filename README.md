 # Personalized Fitness & Nutrition Insight

Sistema end-to-end per la raccolta, elaborazione e visualizzazione di dati fitness e nutrizione, con architettura a microservizi e pipeline Medallion (Bronze → Silver → Gold) su MinIO (S3) e query tramite DuckDB/Delta. Frontend in Streamlit, gateway REST in FastAPI e simulazione sensori via MQTT/Kafka.

## Panoramica
- Ingestion in tempo reale da sensori (MQTT/EMQX) e generatori.
- Inoltro dati su Kafka e persistenza nel Lakehouse su MinIO (bucket S3 compatibile).
- ETL con Spark per Bronze/Silver/Gold.
- Gateway REST (FastAPI) che legge tabelle Delta (Gold) via DuckDB+httpfs.
- Frontend Streamlit per dashboard salute, attività e pasti.

## Architettura (alto livello)
- EMQX (broker MQTT) → Kafka (topic hot/cold)
- Spark jobs:
  - Bronze: consumer da Kafka → scrittura raw/bronze
  - Silver: pulizia/arricchimento
  - Gold: aggregati facts e daily per metrics, meals, activities
- Storage: MinIO
- DB relazionale: CockroachDB (utenti/profili/dispositivi)
- Cache: Redis (+ RedisInsight)
- Gateway REST → legge Delta su MinIO via DuckDB
- Frontend Streamlit → chiama il Gateway

## Servizi e porte
- Streamlit: http://localhost:8501
- Gateway REST: http://localhost:8237 (proxy verso container :8000)
- EMQX dashboard: http://localhost:18083 (user: admin, pass: public)
- MQTT (EMQX): 1883; WebSocket 8083; WSS 8084; TLS 8883
- Kafka UI: http://localhost:8082
- CockroachDB: SQL 26257, Admin UI http://localhost:8087
- MinIO: API 9000, Console http://localhost:9001 (user/pass: minioadmin/minioadmin)
- RedisInsight: http://localhost:5540; Redis: 6379
- Spark Master UI: http://localhost:8085; Spark Master: 7077

## Requisiti
- Docker Desktop o Docker Engine + Docker Compose v2
- 8 GB RAM consigliati (Spark + MinIO + Kafka)
- Porte libere elencate sopra

## Avvio rapido
1. Clona il repository.
2. Avvia tutti i servizi: docker compose up --build
3. Attendi che i container siano up; i topic Kafka verranno creati dal servizio init_kafka.
4. Apri le interfacce:
   - Streamlit: http://localhost:8501
   - Gateway: http://localhost:8237/healthz
   - EMQX: http://localhost:18083
   - Kafka UI: http://localhost:8082
   - MinIO: http://localhost:9001
   - Cockroach UI: http://localhost:8087
   - RedisInsight: http://localhost:5540

Per fermare: docker compose down
Per reset totale (attenzione: cancella i volumi): docker compose down -v

## Variabili d’ambiente principali (Gateway)
Impostate in docker-compose.yml per il servizio gateway:
- MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
- AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY/REGION
- AWS_S3_FORCE_PATH_STYLE=true, AWS_ENDPOINT_URL_S3=http://minio:9000
- KAFKA_BOOTSTRAP=broker_kafka:9092, MEALS_TOPIC=meals, ACTIVITIES_TOPIC=activities

## Topic Kafka (da src/kafka_topics/init_kafka.sh)
- Cold: users_changes, silver_layer, gold_layer, meals
- Hot: activities, wearables.ppg.raw, wearables.skin-temp.raw, wearables.accelerometer.raw, wearables.gyroscope.raw, wearables.altimeter.raw, wearables.barometer.raw, wearables.ceda.raw

## API principali (Gateway REST)
Base URL: http://localhost:8237
- Health: GET /healthz
- Meals: POST /api/meals, DELETE /api/meals/{meal_id}, GET /meals/facts, GET /meals/daily
- Activities: POST /api/activities, DELETE /api/activities/{activity_event_id}, GET /activities/facts, GET /activities/daily
- Metrics: GET /metrics/facts, GET /metrics/daily

Esempi curl
- Aggiungi attività:
  curl -X POST http://localhost:8237/api/activities \
    -H "Content-Type: application/json" \
    -d '{"activity_event_id":"act-1","user_id":"u1","activity_id":1,"activity_name":"Corsa","start_ts":"2025-08-25T06:30:00Z","end_ts":"2025-08-25T07:05:00Z","notes":"parco"}'
- Aggiungi pasto:
  curl -X POST http://localhost:8237/api/meals \
    -H "Content-Type: application/json" \
    -d '{"meal_id":"meal-1","user_id":"u1","meal_name":"Colazione","kcal":350,"carbs_g":45,"protein_g":18,"fat_g":10,"timestamp":"2025-08-25T07:30:00Z"}'
- Letture giornaliere metrics:
  curl "http://localhost:8237/metrics/daily?user_id=u1&start_date=2025-08-25&end_date=2025-08-25&limit=1"

## Frontend
- Streamlit gira su http://localhost:8501 e chiama il Gateway all’indirizzo interno http://gateway:8000 (network Docker).
- Pagine principali: Dashboard (KPI giornalieri e liste), Health (serie temporali metrics), Activities (crea/elimina attività via API).

## Database relazionale (CockroachDB)
- Host interno: cockroachdb:26257, DB: user_device_db, user: root
- Il frontend usa psycopg per registrazione/login e profilo (utility/database_connection.py)

## Storage (MinIO) e Lakehouse
- Bucket S3 (es. gold) contenente tabelle Delta: meals_fact, meals_daily, metrics_fact, metrics_daily, activities_fact, activities_daily
- Il Gateway usa DuckDB con httpfs per leggere le Delta tables direttamente da MinIO

## Struttura cartelle (principali)
- src/
  - frontend/ (Streamlit app; pages/ dashboard.py, health.py, activity.py)
  - gateway/ (FastAPI; app/main.py)
  - delta/
    - bronze/ (consumatori Kafka → Bronze)
    - silver/
    - gold/ (job Gold: sensor, meal, activity)
  - kafka_topics/ (Dockerfile + init_kafka.sh)
  - data_generator/ (fake_user, sensori people_simulator, sensor_coherent_simulator)
  - change_data_capture/
- utility/ (database_connection.py, frontend_utility/ui, ecc.)
- volumes/ (dati persistenti: MinIO, Kafka, Redis, Cockroach)
- database_utility/ (es. output utenti finti)
- docker-compose.yml, requirements.txt, init_files/

## Volumi
Il progetto usa host volumes per persistere i dati su ./volumes/* e per mappare codice del frontend dentro il container.

## Troubleshooting
- Porte occupate: modifica il mapping nel docker-compose.yml o libera la porta.
- Kafka topics mancanti: verifica che init_kafka sia partito dopo broker_kafka.
- EMQX non raggiungibile: attendi healthcheck o controlla credenziali admin/public.
- MinIO errori S3: verifica variabili d’ambiente del gateway e che la console sia attiva.
- Performance: aumenta risorse Docker (RAM/CPU) per Spark.
- Reset ambiente: docker compose down -v (perdi i dati!).

## Sviluppo
- Modifica il codice del frontend in src/frontend: la cartella è montata nel container per reload veloce.
- Per il gateway, rebuild dopo modifiche: docker compose build gateway && docker compose up gateway
