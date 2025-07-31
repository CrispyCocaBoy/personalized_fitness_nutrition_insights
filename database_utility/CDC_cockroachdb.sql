USE user_device_db;

-- Vista solo per chiarezza, non obbligatoria
CREATE VIEW sensor_user_mapping AS
SELECT sensor_id, user_id
FROM sensor_to_user;

-- Changefeed che manda solo INSERT e DELETE
CREATE CHANGEFEED FOR TABLE sensor_user_mapping
INTO 'kafka://broker_kafka:9092?topic_name=users_changes'
WITH
    cursor = 'now()',        -- Parti dai nuovi eventi
    key_column = 'sensor_id',-- Kafka key = sensor_id
    deleted,                 -- Invia eventi su DELETE
    envelope = 'wrapped';    -- Prima/Dopo per distinguere insert e delete
