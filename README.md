# Personalized Fitness & Nutrition Insight

The project develops a Big Data system that integrates data from wearable devices, nutrition apps and user demographics.

Through streaming pipelines and machine learning algorithms, it delivers personalized fitness and nutrition recommendations that adapt in real time to users’ behavior and progress.

Key objectives:
- Build a real-time data pipeline that manages different types of data: sensor-based inputs from wearables and self-reported data from users.
- Develop a personalized recommendation engine that continuously adapts to user adherence and progress.

Main results achieved:
- Designed and implemented a real-time streaming pipeline using MQTT, Kafka, Spark Structured Streaming, Redis, CockroachDB and MinIO, organized across different storage layers.  
- Developed a recommendation algorithm for workouts and nutrition plans, leveraging both users’ historical data and collective feedback from other users to continuously refine suggestions.  
- Built an interactive dashboard that provides multiple functionalities, including real-time monitoring and visualization of key health and fitness metrics and sensor management.

## Key Features
- 📡 **Multi-Sensor Integration** – Connect and manage multiple wearable devices and individual sensors simultaneously.
- ⚡ **Real-Time Processing** – Stream and process sensor events (heart rate, steps, sleep patterns, meals) using Kafka and Spark.
- 📊 **Multiple storage** – Store raw, processed, and enriched data with support for incremental updates (Bronze/Silver/Gold layers). Cache data on redis, and save settings and user feature on cockroach
- 🧠 **Machine Learning Recommendations** – Adaptive models generate personalized workout and meal suggestions, refined through user feedback.
- 🖥️ **Interactive Dashboard** – Visualize fitness trends, nutrition balance, and sensors and wearables settings. 
- 🔄 **Feedback Loop** – Continuous improvement of recommendations based on user interactions and goal tracking.


## 🏗️ System Architecture
This system integrates multiple data sources — both **semi-structured** (activity logs, nutrition logs, wearable sensor data) and **structured** (profile, demographics, feedback, device settings, default options) — into a unified data pipeline for real-time analytics and personalized recommendations.

### 📥 Ingestion Layer
The ingestion layer is the entry point for all data streams:
- **Semi-structured data**:
    - _Activity Logs_ (daily steps, workouts, energy expenditure)
    - _Nutrition Logs_ (meals, calories, macronutrients)
    - _Sensor Data_ (heart rate, sleep, biometrics in real time)

- **Structured data**:
    - _Device Settings Data_ (wearable configuration parameters)
    - _Profile Data_ (user metadata: age, weight, height, gender)
    - _Demographic Data_ (population-level info such as region, gender distribution)
    - _Feedback Data_ (user preferences, likes/dislikes on recommendations)
    - _Default Options_ (fallback values for configurations and personalization)

Semi-structured data flows through **streaming brokers** (e.g., **Kafka** or **MQTT**) for near-real-time processing, and get structured via the delta-table format in a datalakehouse in minio.  
Structured data is ingested from **relational databases**. 

---

### ⚙️ Processing Layer
The processing layer handles real-time transformations, cleaning, and enrichment of ingested data
- Streaming jobs (e.g., **Spark Structured Streaming**) validate, normalize, and pre-aggregate activity, nutrition, and sensor data.
- Processing ensures quality, consistency, and prepares data for downstream storage.
- Domain-specific transformations are applied (e.g., unit conversion, outlier filtering, activity/nutrition enrichment).


### 🗄️ Storage Layer

This layer provides **durable, structured storage** for both raw and processed data:
- Semi-structured logs are persisted into **datalakehouse** in a objcet store, minio in a delta table format
- Structured datasets are stored in **relational stores** (CockroachDB) for transactional queries.
- Redis or caching layers may be used for fast access to device configurations or session-based values.

---

### 🔗 Aggregation Layer
At this stage, diverse data sources are **combined and aligned** to provide a holistic view of the user:
- Activity, nutrition, sensor streams are joined with profile, demographic, and device data.
- Feedback loops are integrated to refine the aggregation (user adherence, preferences).
- Aggregated datasets are stored in curated tables for direct consumption by models.

### 🤖 Model Layer

This layer is responsible for **personalization and intelligence**
- Machine learning models (trained on historical user clusters, behavior, and demographics) generate:
    - Personalized **workout recommendations**
    - Personalized **nutrition plans**
- Feedback data is continuously integrated to retrain/update models (adaptive learning).
- Models are deployed as services, accessible through APIs for the frontend.

###  🚀 Serving Layer
The **Serving Layer** is the bridge between storage systems (SQL + object store) and the UI (Streamlit). It provides **low-latency APIs** that expose KPIs, recommendations, and model outputs to the frontend. It is created with fastAPI

### 📊 Visualization Layer

The final layer delivers insights and recommendations back to the end-user:
- A **dashboard (Streamlit or web frontend)** displays:
    - Daily energy balance (calories consumed vs. burned)
    - Progress on workouts and nutrition adherence
    - Personalized recommendations (workouts, meals)
- Users can provide **feedback** directly in the UI, which flows back into the ingestion layer to improve future recommendations.
- The user can login, logout, sign in, change settings, change sensor or wearables and much more



## 🚀 Getting Started

Install docker desktop app

```bash
# Clone repository
git clone https://github.com/your-repo/fitness-nutrition-insight.git
cd fitness-nutrition-insight

# Start services with our entrypoint 
./entrypoint.sh

# Use command of docker
docker-compose up --build
```

The platform will start all required services (Kafka, MinIO, Spark, databases, frontend) and make the dashboard available at http://localhost:8501

Please use one of our account:
- Username: luca.bianchi',  
- password: 'password1',
- E-mail: luca.bianchi@example.com

## How to access to the UI services  
- Streamlit: http://localhost:8501  
- Gateway REST: http://localhost:8237 (proxy verso container :8000)  
- EMQX dashboard: http://localhost:18083 (user: admin, pass: public)  
- MQTT (EMQX): 1883; WebSocket 8083; WSS 8084; TLS 8883  
- Kafka UI: http://localhost:8082  
- CockroachDB: SQL 26257, Admin UI http://localhost:8087  
- MinIO: API 9000, Console http://localhost:9001 (user/pass: minioadmin/minioadmin)  
- RedisInsight: http://localhost:5540; Redis: 6379  
- Spark Master UI: http://localhost:8085; Spark Master: 7077


> [!warning] IMPORTANT
> DO NOT DELETE THE VOLUMES INSIDE THE REPOSITORY
