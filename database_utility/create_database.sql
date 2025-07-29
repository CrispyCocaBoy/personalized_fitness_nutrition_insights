-- 1. USERS
--    Stores the login credentials of the user
CREATE TABLE users (
  user_id        SERIAL PRIMARY KEY,
  username       VARCHAR(50) NOT NULL UNIQUE,
  password       VARCHAR(255) NOT NULL,
  email          VARCHAR(100) NOT NULL UNIQUE,
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. USERS_PROFILE
--    Stores additional demographic information about the user
CREATE TABLE users_profile (
  user_id        INT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
  name           VARCHAR(255),
  surname        VARCHAR(255),
  gender         VARCHAR(50),
  birthday       DATE,
  height         NUMERIC(5,2),
  country        VARCHAR(255),
  updated_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. WEIGHT
--    Historical weight data for each user
CREATE TABLE weight (
  weight_id      SERIAL PRIMARY KEY,
  user_id        INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  weight         NUMERIC(5,2) NOT NULL,
  measured_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. DEVICE_TYPE
--    Supported device types (watch, band, ring, phone...)
CREATE TABLE device_type (
  device_type_id SERIAL PRIMARY KEY,
  name           VARCHAR(50) NOT NULL UNIQUE,
  manufacturer   VARCHAR(50),
  model          VARCHAR(50),
  description    TEXT
);

-- 5. DEVICE
--    Physical devices registered by a user
CREATE TABLE device (
  device_id         SERIAL PRIMARY KEY,
  user_id           INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  device_name       VARCHAR(100) NOT NULL,
  device_type_id    INT NOT NULL REFERENCES device_type(device_type_id) ON DELETE CASCADE,
  serial_number     VARCHAR(100),
  registered_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(user_id, serial_number)
);

-- 6. SENSOR_TYPE (HARDWARE)
--    Physical sensors supported by the system
CREATE TABLE sensor_type (
  sensor_type_id SERIAL PRIMARY KEY,
  name           VARCHAR(50) NOT NULL UNIQUE,
  unit           VARCHAR(20) NOT NULL,
  description    TEXT
);

-- 7. FEATURE (DERIVED METRICS)
--    Features/metrics that can be calculated from sensors
CREATE TABLE feature (
  feature_id     SERIAL PRIMARY KEY,
  name           VARCHAR(50) NOT NULL UNIQUE,
  unit           VARCHAR(20),
  sensor_type_id INT NOT NULL REFERENCES sensor_type(sensor_type_id),
  description    TEXT
);

-- 8. PREDEFINED_DEVICE_TYPE_SENSORS
--    Which physical sensors are mounted on each device type
CREATE TABLE predefined_device_type_sensors (
  device_type_id INT NOT NULL REFERENCES device_type(device_type_id) ON DELETE CASCADE,
  sensor_type_id INT NOT NULL REFERENCES sensor_type(sensor_type_id) ON DELETE CASCADE,
  priority       INT NOT NULL DEFAULT 100,
  PRIMARY KEY (device_type_id, sensor_type_id)
);

-- 9. SENSOR_TO_USER
--    Maps physical sensors to user devices
CREATE TABLE sensor_to_user (
  sensor_id      SERIAL PRIMARY KEY,
  device_id      INT NOT NULL REFERENCES device(device_id) ON DELETE CASCADE,
  sensor_type_id INT NOT NULL REFERENCES sensor_type(sensor_type_id) ON DELETE CASCADE,
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(device_id, sensor_type_id)
);


-- 10. USER_FEATURE_PREFERENCES
--     Stores which features each user wants to monitor
CREATE TABLE user_feature_preferences (
  user_id        INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  feature_id     INT NOT NULL REFERENCES feature(feature_id) ON DELETE CASCADE,
  device_id      INT NOT NULL REFERENCES device(device_id) ON DELETE CASCADE,
  updated_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (user_id, feature_id, device_id)
);

---------------------------------------------------------
-- STANDARD SETUP
-- These are initial/default values for device types, sensors, and features
---------------------------------------------------------

-- Device types
INSERT INTO device_type (name, manufacturer, model, description)
VALUES
  ('SimpleWatch', 'simpleguys', 'SW_001', 'Basic smartwatch'),
  ('SimpleBand',  'simpleguys', 'SB_001', 'Basic smartband'),
  ('SimpleRing',  'simpleguys', 'SR_001', 'Smart ring'),
  ('Phone',       'generic',    NULL,     'Generic smartphone');

-- Sensor types (hardware)
INSERT INTO sensor_type (name, unit, description) VALUES
('PPG', 'ADC', 'Raw PPG signal from green/red/IR LEDs'),
('SkinTemp', '°C', 'Skin temperature sensor'),
('Accelerometer', 'm/s²', '3-axis accelerometer'),
('Gyroscope', '°/s', '3-axis gyroscope'),
('Altimeter', 'm', 'Altitude estimated from pressure'),
('Barometer', 'hPa', 'Atmospheric pressure'),
('cEDA', 'µS', 'Electrodermal activity for stress monitoring');

-- Features (derived metrics)
INSERT INTO feature (name, unit, description) VALUES
('bpm', 'bpm', 'Heart rate'),
('hrv', 'ms', 'Heart rate variability'),
('spo2', '%', 'Oxygen saturation'),
('steps', 'count', 'Estimated steps'),
('calories', 'kcal', 'Estimated calories burned'),
('skin_temp', '°C', 'Average skin temperature');

-- Default device -> sensors mapping
-- SimpleWatch: all sensors
INSERT INTO predefined_device_type_sensors (device_type_id, sensor_type_id, priority) VALUES
(1, 1, 1),
(1, 2, 1),
(1, 3, 1),
(1, 4, 1),
(1, 5, 1),
(1, 6, 1),
(1, 7, 1);

-- SimpleBand: only subset
INSERT INTO predefined_device_type_sensors (device_type_id, sensor_type_id, priority) VALUES
(2, 1, 1),
(2, 3, 1),
(2, 7, 1);

-- SimpleRing: only PPG + SkinTemp
INSERT INTO predefined_device_type_sensors (device_type_id, sensor_type_id, priority) VALUES
(3, 1, 1),
(3, 2, 1);

-- Phone: only accelerometer + gyroscope
INSERT INTO predefined_device_type_sensors (device_type_id, sensor_type_id, priority) VALUES
(4, 3, 1),
(4, 4, 1);

