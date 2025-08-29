USE user_device_db;


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

-- Emissione eventi per ogni INSERT/UPDATE/DELETE su sensor_status
CREATE CHANGEFEED FOR TABLE sensor_status
INTO 'kafka://broker_kafka:9092?topic_name=toggle_sensor'
WITH
    cursor = 'now()',
    key_column = 'sensor_id',
    format = 'json',
    envelope = 'wrapped',
    diff,          -- include "before" negli UPDATE
    unordered;
