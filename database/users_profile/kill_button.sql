-- ATTENZIONE: questa operazione cancellerà tutti i dati utente!

-- Ordine corretto per evitare errori FK
TRUNCATE TABLE
    user_feature_preferences,
    sensor,
    predefined_device_type_sensors,
    feature,
    device,
    weight,
    users_profile,
    users
RESTART IDENTITY CASCADE;
