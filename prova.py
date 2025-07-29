import psycopg
import random
import string

def connection():
    return psycopg.connect(
        host="cockroachdb",
        port=26257,
        dbname="user_device_db",
        user="root"
        #user="admin",
        #password="admin",
        #sslmode="require"  # CockroachDB spesso richiede SSL
    )






def bind_device(user_id = 1, device_type_id = 1, device_type_name = 'prova'):
    conn = connection()
    cur = conn.cursor()

    try:
        # Recupero nome utente
        cur.execute("SELECT name FROM users_profile WHERE user_id = %s", (user_id,))
        user_name = cur.fetchone()

        device_name = f"{device_type_name} of {user_name[0]}"
        serial_number = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))

        # Inserimento device e recupero device_id
        cur.execute("""
                    INSERT INTO device (user_id, device_name, device_type_id, serial_number, registered_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING device_id
                    """, (user_id, device_name, device_type_id, serial_number))

        device_id = cur.fetchone()[0]

        # Recupero sensori collegati al device type
        cur.execute("""
                    SELECT sensor_type_id
                    FROM predefined_device_type_sensors
                    WHERE device_type_id = %s
                    """, (device_type_id,))
        sensors_to_generate = [row[0] for row in cur.fetchall()]

        # Inserimento sensori collegati al device
        for sensor_type_id in sensors_to_generate:
            cur.execute("""
                        INSERT INTO sensor_to_user (device_id, user_id, sensor_type_id, created_at)
                        VALUES (%s, %s, %s, NOW())
                        """, (device_id, user_id, sensor_type_id))

        # Commit finale
        conn.commit()
        return True, f"Device {device_name} con sensori {sensors_to_generate} creato con successo!"

    except Exception as e:
        conn.rollback()
        return False, f"Errore: {e}"

    finally:
        conn.close()

print(bind_device())