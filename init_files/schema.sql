-- Imposta il database corrente
SET database = user_device_db;

-- Tabella per gli utenti
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY DEFAULT unique_rowid(),
    username STRING UNIQUE NOT NULL,
    email STRING UNIQUE,
    password STRING NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Tabella per i profili utente
CREATE TABLE IF NOT EXISTS users_profile (
    profile_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INT UNIQUE NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    name STRING,
    surname STRING,
    gender STRING,
    birthday DATE,
    country STRING,
    height FLOAT,
    weight FLOAT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Tabella per le raccomandazioni generate
CREATE TABLE IF NOT EXISTS user_workout_rankings (
    ranking_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INT NOT NULL,
    recommendation_id INT NOT NULL,
    workout_type STRING,
    details STRING,
    success_prob FLOAT NOT NULL,
    rank INT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL
);

-- Tabella per la blacklist degli utenti
CREATE TABLE IF NOT EXISTS user_recommendation_blacklist (
    blacklist_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INT NOT NULL,
    recommendation_id INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Tabella per tracciare i feedback positivi
CREATE TABLE IF NOT EXISTS user_recommendation_feedback (
    feedback_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INT NOT NULL,
    recommendation_id INT NOT NULL,
    feedback_type STRING DEFAULT 'positive',
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Pulisce la vecchia tabella per evitare confusione
DROP TABLE IF EXISTS user_workout_recommendations;
