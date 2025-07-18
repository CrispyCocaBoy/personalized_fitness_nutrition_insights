CREATE TABLE users (
  user_id        SERIAL PRIMARY KEY,
  username       VARCHAR(50) NOT NULL UNIQUE,
  password       VARCHAR(255) NOT NULL,
  email          VARCHAR(100) NOT NULL UNIQUE,
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- (no FKs)

-- 2. USER_PROFILE
CREATE TABLE users_profile (
  user_id        INT        PRIMARY KEY,
  name           VARCHAR(255),
  surname        VARCHAR(255),
  gender         VARCHAR(255),
  birthday       DATE,
  height         NUMERIC(5,2),
  updated_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- USER_PROFILE(user_id) → USER(user_id)

-- 3. WEIGHT
CREATE TABLE weight (
  weight_id      SERIAL PRIMARY KEY,
  user_id        INT    NOT NULL,
  weight         NUMERIC(5,2) NOT NULL,
  measured_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- WEIGHT(user_id) → USER(user_id)

-- 4. DEVICE_TYPE
CREATE TABLE device_type (
  device_type_id SERIAL PRIMARY KEY,
  name           VARCHAR(50) NOT NULL UNIQUE,
  manufacturer   VARCHAR(50),
  model          VARCHAR(50),
  description    TEXT
);
-- (no FKs)

-- 5. DEVICE
CREATE TABLE device (
  device_id         SERIAL PRIMARY KEY,
  user_id           INT    NOT NULL,
  device_name       VARCHAR(100) NOT NULL,
  device_type_name  VARCHAR(50)  NOT NULL,
  serial_number     VARCHAR(100),
  registered_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- DEVICE(user_id) → USER(user_id)
-- DEVICE(device_type_name) → DEVICE_TYPE(name)

-- 6. FEATURE
CREATE TABLE feature (
  feature_id     SERIAL PRIMARY KEY,
  name           VARCHAR(50) NOT NULL UNIQUE,
  unit           VARCHAR(20) NOT NULL,
  description    TEXT
);
-- (no FKs)

-- 7. PREDEFINED_DEVICE_TYPE_SENSORS
CREATE TABLE predefined_device_type_sensors (
  device_type_id INT NOT NULL,
  feature_id     INT NOT NULL,
  priority       INT NOT NULL DEFAULT 100,
  PRIMARY KEY (device_type_id, feature_id)
);
-- PREDEFINED_DEVICE_TYPE_SENSORS(device_type_id) → DEVICE_TYPE(device_type_id)
-- PREDEFINED_DEVICE_TYPE_SENSORS(feature_id)     → FEATURE(feature_id)

-- 8. SENSOR
CREATE TABLE sensor (
  sensor_id      SERIAL PRIMARY KEY,
  device_id      INT    NOT NULL,
  feature_id     INT    NOT NULL,
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(device_id, feature_id)
);
-- SENSOR(device_id)  → DEVICE(device_id)
-- SENSOR(feature_id) → FEATURE(feature_id)

-- 9. USER_FEATURE_PREFERENCES
CREATE TABLE user_feature_preferences (
  user_id        INT NOT NULL,
  feature_id     INT NOT NULL,
  device_id      INT NOT NULL,
  updated_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (user_id, feature_id)
);
-- USER_FEATURE_PREFERENCES(user_id)    → USER(user_id)
-- USER_FEATURE_PREFERENCES(feature_id) → FEATURE(feature_id)
-- USER_FEATURE_PREFERENCES(device_id)  → DEVICE(device_id)

--- Impostazione device
INSERT INTO device_type (name, manufacturer, model, description)
VALUES
  ('SimpleWatch', 'simpleguys', 'SW_001', 'smartwatch'),
  ('SimpleBand', 'simpleguys', 'SB_001', 'smartband'),
  ('SimpleRing', 'simpleguys', 'SR_001', 'smartring'),
  ('Phone', '','','');
-- Feature di calcolo --
INSERT INTO feature (name, unit, description) VALUES
-- Cardiovascolari
    ('bpm', 'beats/min', 'Frequenza cardiaca calcolata dal sensore PPG'),
    ('hrv', 'ms', 'Heart Rate Variability: variazione tra intervalli tra battiti'),

-- Respirazione & SpO₂
    ('spo2', '%', 'Saturazione dell’ossigeno nel sangue stimata con LED rossi e infrarossi'),

-- Movimento
    ('steps', 'count', 'Numero di passi effettuati'),

--Temperatura
    ('skin_temp', '°C', 'Temperatura cutanea rilevata');

-- Mappatura device -> sensor
-- Assunzioni:
-- device_type_id:
-- 1 = SimpleWatch, 2 = SimpleBand, 3 = SimpleRing, 4 = Phone
-- feature_id:
-- 1 = bpm, 2 = hrv, 3 = spo2, 4 = steps, 5 = skin_temp

-- SimpleWatch → tutte le feature
INSERT INTO predefined_device_type_sensors (device_type_id, feature_id, priority) VALUES
(1, 1, 1),  -- bpm
(1, 2, 1),  -- hrv
(1, 3, 1),  -- spo2
(1, 4, 1),  -- steps
(1, 5, 1);  -- skin_temp

-- SimpleBand → tutte tranne skin_temp
INSERT INTO predefined_device_type_sensors (device_type_id, feature_id, priority) VALUES
(2, 1, 1),  -- bpm
(2, 2, 1),  -- hrv
(2, 3, 1),  -- spo2
(2, 4, 1);  -- steps

-- SimpleRing → hrv, spo2, skin_temp
INSERT INTO predefined_device_type_sensors (device_type_id, feature_id, priority) VALUES
(3, 1, 1),  -- bpm
(3, 2, 1),  -- hrv
(3, 3, 1),  -- spo2
(3, 5, 1);  -- skin_temp

-- Phone → bpm, steps
INSERT INTO predefined_device_type_sensors (device_type_id, feature_id, priority) VALUES
(4, 4, 1);  -- steps
