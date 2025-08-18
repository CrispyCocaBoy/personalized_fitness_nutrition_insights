USE user_device_db;

-- Vista solo per chiarezza, non obbligatoria
CREATE VIEW sensor_user_mapping AS
SELECT sensor_id, user_id
FROM sensor_to_user;

SET CLUSTER SETTING kv.rangefeed.enabled = true;

-- Changefeed che manda solo INSERT e DELETE
CREATE CHANGEFEED FOR TABLE sensor_to_user
INTO 'kafka://broker_kafka:9092?topic_name=users_changes'
WITH
    cursor = 'now()',
    key_column = 'sensor_id',
    format = 'json',
    envelope = 'wrapped',
    unordered;
