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
  birthdate      DATE,
  gender         CHAR(1),
  height_cm      NUMERIC(5,2),
  updated_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- USER_PROFILE(user_id) → USER(user_id)

-- 3. WEIGHT
CREATE TABLE weight (
  weight_id      SERIAL PRIMARY KEY,
  user_id        INT    NOT NULL,
  measured_at    TIMESTAMP WITH TIME ZONE NOT NULL,
  value_kg       NUMERIC(5,2) NOT NULL
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

