# PERSONALIZED FITNESS AND NUTRITION INSIGHTS

The project develops a Big Data system that integrates data from wearable devices, nutrition apps and user demographics.

Through streaming pipelines and machine learning algorithms, it delivers personalized fitness and nutrition recommendations that adapt in real time to users’ behavior and progress.

Key objectives:
- Build a real-time data pipeline that manages different types of data: sensor-based inputs from wearables and self-reported data from users.
- Develop a personalized recommendation engine that continuously adapts to user adherence and progress.

Main results achieved:
- Designed and implemented a real-time streaming pipeline using MQTT, Kafka, Spark Structured Streaming, Redis, CockroachDB and MinIO, organized across different storage layers.  
- Developed a recommendation algorithm for workouts and nutrition plans, leveraging both users’ historical data and collective feedback from other users to continuously refine suggestions.  
- Built an interactive dashboard that provides multiple functionalities, including real-time monitoring and visualization of key health and fitness metrics and sensor management.

## Table of Contents

- [Team Composition](#team-composition)
- [Features](#features)
- [Data Pipeline and Infrastructure](#data-pipeline-and-infrastructure)
- [Data Flow Diagram](#data-flow-diagram)
- [Getting Started](#getting-started)
- [Quick Tour](#quick-tour)



## Team Composition

Questo progetto è stato sviluppato dal Gruppo numero X rappresentato da:

1. **Matteo Massari** - matteo.massari@studenti.unitn.it
2. **Andrea Battaglia** - andrea.battaglia-1@studenti.unitn.it
3. **Tommaso Ballarini** - tommaso.ballarini-1@studenti.unitn.it



## Features
-  **Multi-Sensor Integration** – Connect and manage multiple wearable devices and individual sensors simultaneously.
-  **Real-Time Processing** – Stream and process sensor events (heart rate, steps, sleep patterns, meals) using Kafka and Spark.
-  **Multiple storage** – Store raw, processed, and enriched data with support for incremental updates (Bronze/Silver/Gold layers). Cache data on redis, and save settings and user feature on cockroach
-  **Machine Learning Recommendations** – Adaptive models generate personalized workout and meal suggestions, refined through user feedback.
-  **Interactive Dashboard** – Visualize fitness trends, nutrition balance, and sensors and wearables settings. 
-  **Feedback Loop** – Continuous improvement of recommendations based on user interactions and goal tracking.

---

## Data Pipeline and Infrastructure
<img width="2048" height="1399" alt="system architecture" src="https://github.com/user-attachments/assets/034bb336-3f4a-4603-8565-e4b81e95b4cf" />


---

Semi-structured data flows through **streaming brokers** (e.g., **Kafka** or **MQTT**) for near-real-time processing, and get structured via the delta-table format in a datalakehouse in minio.  
Structured data is ingested from **relational databases**. 

---

## Data Flow Diagram
![data flow](https://github.com/user-attachments/assets/ab734db6-c21b-4290-9305-0c960de36899)

---

### Processing Layer
The processing layer handles real-time transformations, cleaning, and enrichment of ingested data
- Streaming jobs (e.g., **Spark Structured Streaming**) validate, normalize, and pre-aggregate activity, nutrition, and sensor data.
- Processing ensures quality, consistency, and prepares data for downstream storage.
- Domain-specific transformations are applied (e.g., unit conversion, outlier filtering, activity/nutrition enrichment).


### Storage Layer

This layer provides **durable, structured storage** for both raw and processed data:
- Semi-structured logs are persisted into **datalakehouse** in a objcet store, minio in a delta table format
- Structured datasets are stored in **relational stores** (CockroachDB) for transactional queries.
- Redis or caching layers may be used for fast access to device configurations or session-based values.

### Aggregation Layer
At this stage, diverse data sources are **combined and aligned** to provide a holistic view of the user:
- Activity, nutrition, sensor streams are joined with profile, demographic, and device data.
- Feedback loops are integrated to refine the aggregation (user adherence, preferences).
- Aggregated datasets are stored in curated tables for direct consumption by models.

### Model Layer

This layer is responsible for **personalization and intelligence**
- Machine learning models (trained on historical user clusters, behavior, and demographics) generate:
    - Personalized **workout recommendations**
    - Personalized **nutrition plans**
- Feedback data is continuously integrated to retrain/update models (adaptive learning).
- Models are deployed as services, accessible through APIs for the frontend.

### Serving Layer
The **Serving Layer** is the bridge between storage systems (SQL + object store) and the UI (Streamlit). It provides **low-latency APIs** that expose KPIs, recommendations, and model outputs to the frontend. It is created with fastAPI

### Visualization Layer

The final layer delivers insights and recommendations back to the end-user:
- A **dashboard (Streamlit or web frontend)** displays:
    - Daily energy balance (calories consumed vs. burned)
    - Progress on workouts and nutrition adherence
    - Personalized recommendations (workouts, meals)
- Users can provide **feedback** directly in the UI, which flows back into the ingestion layer to improve future recommendations.
- The user can login, logout, sign in, change settings, change sensor or wearables and much more
- 


## Getting Started

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

Please use one of our account

-Luca Bianchi:
  - Username: luca.bianchi',  
  - password: 'password1',
  - E-mail: luca.bianchi@example.com
    
---
    
-Marco Verdi:
  - Username: marco.verdi',  
  - password: 'password2',
  - E-mail: marco.verdi@example.com

### How to access to the UI services  
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


## Quick Tour

If you can’t wait for the app to finish building and want to learn more about the dashboard in the meantime, this section will showcase the main features of the fitness tracking application.

### Main Dashboard - Home
<img width="1400" height="938" alt="Screenshot 2025-08-29 at 18 04 11" src="https://github.com/user-attachments/assets/904192da-d9b7-401a-a06b-41cad4554f4e" />


The main screen provides a complete overview of your daily statistics. In the "Today" section, you can see:

- **Steps**: Daily step counter with progress indicators  
- **Avg HR**: Average heart rate of the day
- **Calories In**: Total calories consumed through meals
- **Calories Burned**: Calories burned through physical activity
- **Balance**: Net difference between calories consumed and burned 

The dashboard also includes "Recent Activities" and "Recent Meals" sections for quick access to recent records.

### Meal Management
<img width="1400" height="938" alt="Screenshot 2025-08-29 at 18 03 21" src="https://github.com/user-attachments/assets/e0969699-5b95-41a5-94da-dad419f4660b" />

The Meals section allows you to quickly log meals and monitor calories and macronutrients. Key features include:

- **Quick Add**: Ability to add meals with a "quick add" option for frequent foods  
- **Daily Summary**: View of total daily calories and macronutrients  
- **Meal Details**: Complete list of consumed meals with detailed nutritional information  
- **Historical Trends**: Meal history to monitor eating patterns  

Each meal displays intuitive icons for calories (🔥), carbs (🥖), fats, and proteins (🥩), making nutritional values easy to understand at a glance.

### Activity Tracking
<img width="1400" height="938" alt="Screenshot 2025-08-29 at 18 07 14" src="https://github.com/user-attachments/assets/b8f4c819-d198-4a4f-b3f9-f0a524478c8a" />


The Activities section allows you to log workouts and view your exercise history. Main features include:

- **Quick Add**: Two logging modes – "Quick Add" for instant sessions or "Log Activity" for full details  
- **Wide Range of Sports**: 30 activity types available including Running, Football, Swimming, Gym, Cycling, Tennis, Basketball, Yoga, and many more  
- **Customization**: Each activity can be configured with duration, intensity, and specific parameters  
- **Intuitive Icons**: Each sport has its own distinctive icon for easy identification  

The system supports both cardio and strength training, team and individual sports, ensuring comprehensive fitness tracking.

### Personalized Recommendation System
<img width="1400" height="938" alt="Screenshot 2025-08-29 at 18 07 36" src="https://github.com/user-attachments/assets/327e54dc-e00d-44b8-92d7-2c3b9e41c53a" />


The "Daily Tips" section offers personalized recommendations based on your data and goals:

- **Recommended Workout**: Specific suggestions for physical activity 
- **Recommended Nutrition Plan**: Personalized dietary advice 
- **Feedback System**: "Like" 👍 and "Dislike" 👎 buttons to refine future recommendations  
- **Continuous Personalization**: The system learns from your preferences to improve suggestions over time  

This intelligent recommendation system aims to help maintain motivation and optimize results on your fitness journey.


