import psycopg
import bcrypt
import random
import string

# Connessioni
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

# Register User
def register_user(username, email, password):
    conn = connection()
    cur = conn.cursor()

    # Controllo duplicati
    cur.execute("SELECT * FROM users WHERE username = %s", (username,))
    if cur.fetchone():
        conn.close()
        return False, "Username già esistente.", None

    cur.execute("SELECT * FROM users WHERE email = %s", (email,))
    if cur.fetchone():
        conn.close()
        return False, "Email già esistente.", None

    # Hash password
    hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    try:
        cur.execute(
            "INSERT INTO users (username, email, password) VALUES (%s, %s, %s) RETURNING user_id",
            (username, email, hashed)
        )
        user_id = cur.fetchone()[0]  # Recupera user_id appena creato
        conn.commit()
        conn.close()
        return True, "Registrazione avvenuta con successo.", user_id
    except Exception as e:
        conn.close()
        return False, f"Errore durante la registrazione: {e}", None

# Complete profile
def complete_profile(user_id, name, surname, gender, birthday, country):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users_profile (user_id, name, surname, gender, birthday, country)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (user_id, name, surname, gender, birthday, country))
        conn.commit()
        return True, "Profilo completato con successo!"
    except Exception as e:
        conn.rollback()
        return False, f"Errore durante l'inserimento del profilo: {e}"
    finally:
        conn.close()

# Set weight and height
def set_height(user_id, height):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE users_profile
            SET height = %s
            WHERE user_id = %s
        """, (height, user_id))
        conn.commit()
        return True, "Altezza salvata con successo."
    except Exception as e:
        conn.rollback()
        return False, f"Errore altezza: {e}"
    finally:
        conn.close()

def set_weight(user_id, weight):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO weight (user_id, weight, measured_at)
            VALUES (%s, %s, NOW())
        """, (user_id, weight))
        conn.commit()
        return True, "Peso salvato con successo."
    except Exception as e:
        conn.rollback()
        return False, f"Errore peso: {e}"
    finally:
        conn.close()

# Access
def check_credentials(login_input, password, method):
    try:
        conn = connection()
        cur = conn.cursor()

        # Selezione in base al metodo scelto
        if method == "Username":
            cur.execute("SELECT user_id, password FROM users WHERE username = %s", (login_input,))
        else:  # Email
            cur.execute("SELECT user_id, password FROM users WHERE email = %s", (login_input,))

        result = cur.fetchone()
        conn.close()

        if not result:
            return "not_found", None  # L'utente non esiste

        user_id, hashed_pw = result[0], result[1].encode('utf-8')

        # Verifica della password
        if bcrypt.checkpw(password.encode('utf-8'), hashed_pw):
            return "success", user_id
        else:
            return "wrong_password", None

    except Exception as e:
        return "error", None


def retrive_name(user_id):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
                    SELECT name, surname
                    FROM users_profile
                    WHERE user_id = %s
                    """, (user_id,))
        row = cur.fetchone()

        if row:
            name, surname = row
            return name, surname
        else:
            return None, None  # utente non trovato
    except Exception as e:
        conn.rollback()
        return False, f"Errore nome: {e}"
    finally:
        cur.close()
        conn.close()


# The device selection can be done
# - Via randomization
def random_selection():
    # Connection
    conn = connection()
    cur = conn.cursor()

    # Device random selection
    cur.execute("SELECT device_type_id, name FROM device_type")
    available_types = cur.fetchall()
    device_type_id, device_type_name = random.choice(available_types)
    conn.close
    return device_type_id, device_type_name

# - Via selection in the streamlit app
def stream_selection():
    # Connection
    conn = connection()
    cur = conn.cursor()

    # Device random selection
    cur.execute("SELECT device_type_id, name FROM device_type")
    available_types = cur.fetchall()
    conn.close
    return available_types


def bind_device(user_id: int, device_type_id: int, device_type_name: str, custom_device_name: str | None = None):
    conn = connection()
    cur = conn.cursor()

    try:
        # Recupero nome utente
        cur.execute("SELECT name FROM users_profile WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        if not row:
            return False, f"Utente {user_id} non trovato."
        user_name = row[0]

        # Nome dispositivo: custom oppure di default
        device_name = custom_device_name.strip() if custom_device_name else f"{device_type_name} of {user_name}"

        # Serial number random
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
        return True, f"Device '{device_name}' con sensori {sensors_to_generate} creato con successo!"

    except Exception as e:
        conn.rollback()
        return False, f"Errore: {e}"

    finally:
        conn.close()


def bind_single_sensor(user_id: int, sensor_type_id: int, custom_name: str | None = None):
    conn = connection()
    cur = conn.cursor()
    try:
        # utente esiste?
        cur.execute("SELECT 1 FROM users WHERE user_id=%s", (user_id,))
        if not cur.fetchone():
            return False, f"Utente {user_id} non trovato."

        # sensore esiste?
        cur.execute("SELECT name FROM sensor_type WHERE sensor_type_id=%s", (sensor_type_id,))
        row = cur.fetchone()
        if not row:
            return False, f"Sensor type {sensor_type_id} non trovato."
        sensor_type_name = row[0]

        # Nome sensore: custom oppure di default = sensor_type_name
        sensor_name_final = custom_name.strip() if custom_name else sensor_type_name

        # recupera/crea virtual device
        cur.execute("""
            SELECT d.device_id
            FROM device d
            WHERE d.user_id=%s AND d.device_name='SingleSensor'
            LIMIT 1
        """, (user_id,))
        r = cur.fetchone()
        if r:
            device_id = r[0]
        else:
            cur.execute("SELECT device_type_id FROM device_type WHERE name='VirtualDevice' LIMIT 1")
            r2 = cur.fetchone()
            virtual_type_id = r2[0] if r2 else None

            cur.execute("""
                INSERT INTO device (user_id, device_name, device_type_id, serial_number, registered_at)
                VALUES (%s, 'SingleSensor', %s, CONCAT('VIRTUAL-', FLOOR(RANDOM()*1e9)::text), NOW())
                RETURNING device_id
            """, (user_id, virtual_type_id))
            device_id = cur.fetchone()[0]

        # associazione
        cur.execute("""
            INSERT INTO sensor_to_user (device_id, user_id, sensor_type_id, created_at, custom_name)
            VALUES (%s, %s, %s, NOW(), %s)
            ON CONFLICT DO NOTHING
        """, (device_id, user_id, sensor_type_id, sensor_name_final))

        conn.commit()
        return True, f"Sensore '{sensor_name_final}' ({sensor_type_name}) associato all'utente {user_id}."
    except Exception as e:
        conn.rollback()
        return False, f"Errore: {e}"
    finally:
        conn.close()




# Retrive default_meal
def default_food():
    conn = connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT food_id, name, calories, carbohydrates, protein, fat, quantity, unit, category
        FROM default_foods
        ORDER BY name;
    """)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return [dict(zip(colnames, row)) for row in rows]

def get_food_by_name(food_name: str) -> dict:
    conn = connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM default_foods WHERE name = %s;", (food_name,))
    row = cur.fetchone()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return dict(zip(colnames, row)) if row else None


def get_personalized_food_by_name(user_id: str, food_name: str) -> dict:
    conn = connection()
    cur = conn.cursor()

    cur.execute("""
                SELECT *
                FROM user_foods
                WHERE name = %s
                  AND user_id = %s
                LIMIT 1;
                """, (food_name, user_id))

    row = cur.fetchone()
    colnames = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return dict(zip(colnames, row)) if row else None


def personalized_food(user_id: int):
    conn = connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT user_food_id, name, quantity, unit,
               calories, carbohydrates, protein, fat, fiber, sugars,
               saturated_fat, trans_fat, cholesterol, potassium, iron,
               vitamin_c, vitamin_a, category
        FROM user_foods
        WHERE user_id = %s
        ORDER BY user_food_id DESC;
    """, (user_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    foods = []
    for r in rows:
        foods.append({
            "user_food_id": r[0],
            "name": r[1],
            "quantity": r[2],
            "unit": r[3],
            "calories": r[4],
            "carbohydrates": r[5],
            "protein": r[6],
            "fat": r[7],
            "fiber": r[8],
            "sugars": r[9],
            "saturated_fat": r[10],
            "trans_fat": r[11],
            "cholesterol": r[12],
            "potassium": r[13],
            "iron": r[14],
            "vitamin_c": r[15],
            "vitamin_a": r[16],
            "category": r[17],
        })
    return foods




def insert_personalized_food(user_id: str, food: dict):

    sql = """
          INSERT INTO user_foods (user_id, name, quantity, unit, \
                                  calories, carbohydrates, protein, fat, fiber, sugars, \
                                  saturated_fat, trans_fat, cholesterol, potassium, iron, \
                                  vitamin_c, vitamin_a, category) \
          VALUES (%s, %s, %s, %s, \
                  %s, %s, %s, %s, %s, %s, \
                  %s, %s, %s, %s, %s, \
                  %s, %s, %s)
          RETURNING name; \
          """

    params = (
        user_id,
        (food.get("name") or "").strip(),
        food.get("quantity"),
        (food.get("unit") or None),
        int(food.get("calories") or 0),
        int(food.get("carbohydrates") or 0),
        int(food.get("protein") or 0),
        int(food.get("fat") or 0),
        int(food.get("fiber") or 0),
        int(food.get("sugars") or 0),
        int(food.get("saturated_fat") or 0),
        int(food.get("trans_fat") or 0),
        int(food.get("cholesterol") or 0),
        int(food.get("potassium") or 0),
        int(food.get("iron") or 0),
        int(food.get("vitamin_c") or 0),
        int(food.get("vitamin_a") or 0),
        (food.get("category") or None),
    )

    conn = connection()
    cur = conn.cursor()
    cur.execute(sql, params)
    inserted_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return str(inserted_id)

def delete_personalized_food(user_food_id: int, user_id: int) -> bool:
    """
    Cancella una riga da user_foods in modo sicuro (verifica user_id).
    Ritorna True se ha cancellato qualcosa, False se non ha trovato nulla.
    """
    sql = """
        DELETE FROM user_foods
        WHERE user_food_id = %s AND user_id = %s
        RETURNING user_food_id;
    """

    conn = connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (user_food_id, user_id))
            deleted = cur.fetchone()
        conn.commit()
        return deleted is not None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# Activity calls
def activity_default():
    """
    Recupera la lista delle attività di default dalla tabella activity_default.
    Restituisce una lista di dict con chiavi: activity_id, name, icon.
    """
    conn = connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT activity_id, name, icon
        FROM activity_default
        ORDER BY activity_id;
    """)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return [dict(zip(colnames, row)) for row in rows]

def get_device_types():
    """
    Ritorna [(device_type_id, name)] dai device type disponibili,
    escludendo il tipo 'VirtualDevice'.
    """
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT device_type_id, name
            FROM device_type
            WHERE name <> 'VirtualDevice'
            ORDER BY name ASC
        """)
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        conn.close()


def get_sensors_for_device_type(device_type_id: int):
    """
    Ritorna [(sensor_type_id, sensor_name, priority)] per il device_type_id indicato.
    (Nessuna modifica qui: 'VirtualDevice' non ci arriva perché filtrato a monte.)
    """
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT st.sensor_type_id, st.name, pdts.priority
            FROM predefined_device_type_sensors AS pdts
            JOIN device_type AS dt
              ON dt.device_type_id = pdts.device_type_id
            JOIN sensor_type AS st
              ON st.sensor_type_id = pdts.sensor_type_id
            WHERE dt.device_type_id = %s
            ORDER BY pdts.priority ASC, st.name ASC
        """, (device_type_id,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def list_device_types():
    """
    Ritorna [(device_type_id, device_type_name)] da 'predefined_device_type',
    escludendo 'VirtualDevice'.
    """
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT device_type_id, device_type_name
            FROM predefined_device_type
            WHERE device_type_name <> 'VirtualDevice'
            ORDER BY device_type_name ASC
        """)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def list_sensors_for_device_type(device_type_id: int):
    """
    Ritorna [(sensor_type_id, sensor_name)] per un device type.
    (Nessuna modifica qui.)
    """
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT st.sensor_type_id, st.name
            FROM predefined_device_type_sensors pdts
            JOIN sensor_type st ON st.sensor_type_id = pdts.sensor_type_id
            WHERE pdts.device_type_id = %s
            ORDER BY st.name ASC
        """, (device_type_id,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def list_user_devices(user_id: int):
    """
    Ritorna [(device_id, device_name)] dell'utente,
    escludendo i device 'VirtualDevice' e/o il contenitore 'SingleSensor'.
    """
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT d.device_id, d.device_name
            FROM device d
            LEFT JOIN device_type dt ON dt.device_type_id = d.device_type_id
            WHERE d.user_id = %s
              AND COALESCE(dt.name, '') <> 'VirtualDevice'   -- nasconde i device virtuali
              AND d.device_name <> 'SingleSensor'            -- paracadute se type mancante
            ORDER BY d.registered_at DESC
        """, (user_id,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

def list_all_sensor_types():
    """
    Ritorna [(sensor_type_id, name)] per tutti i sensori disponibili.
    """
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT sensor_type_id, name
            FROM sensor_type
            ORDER BY name ASC
        """)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def list_user_bound_sensors(user_id: int):
    """
    Ritorna le associazioni sensori dell'utente:
    [(sensor_type_id, sensor_type_name, device_id, device_name, custom_name, created_at)]
    """
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                stu.sensor_type_id,
                st.name         AS sensor_type_name,
                stu.device_id,
                d.device_name,
                stu.custom_name,
                stu.created_at
            FROM sensor_to_user AS stu
            JOIN sensor_type AS st ON st.sensor_type_id = stu.sensor_type_id
            JOIN device      AS d  ON d.device_id      = stu.device_id
            WHERE stu.user_id = %s
            ORDER BY st.name ASC
        """, (user_id,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

def rename_user_sensor(sensor_id: int, user_id: int, new_name: str) -> tuple[bool, str]:
    """
    Aggiorna il nome personalizzato (custom_name) di un sensore già associato all'utente.
    Ritorna (ok, msg).
    """
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE sensor_to_user
            SET custom_name = %s
            WHERE sensor_id = %s AND user_id = %s
            RETURNING sensor_id;
        """, (new_name.strip() if new_name else None, sensor_id, user_id))
        updated = cur.fetchone()
        conn.commit()

        if updated:
            return True, f"Nome del sensore {sensor_id} aggiornato a '{new_name}'."
        else:
            return False, f"Nessun sensore trovato con id {sensor_id} per l'utente {user_id}."
    except Exception as e:
        conn.rollback()
        return False, f"Errore: {e}"
    finally:
        cur.close()
        conn.close()

def delete_user_sensor(sensor_id: int, user_id: int):
    conn = connection(); cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM sensor_to_user
             WHERE sensor_id = %s AND user_id = %s
             RETURNING sensor_id
        """, (sensor_id, user_id))
        ok = cur.fetchone()
        conn.commit()
        return (ok is not None, "Sensore eliminato." if ok else "Sensore non trovato o non tuo.")
    except Exception as e:
        conn.rollback(); return False, f"Errore: {e}"
    finally:
        cur.close(); conn.close()

def list_user_bound_sensors_full(user_id: int):
    conn = connection(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                stu.sensor_id,
                stu.sensor_type_id,
                st.name         AS sensor_type_name,
                stu.device_id,
                d.device_name,
                stu.custom_name,
                stu.created_at,
                COALESCE(ss.active, FALSE) AS is_active
            FROM sensor_to_user AS stu
            JOIN sensor_type AS st ON st.sensor_type_id = stu.sensor_type_id
            JOIN device      AS d  ON d.device_id      = stu.device_id
            LEFT JOIN sensor_status AS ss ON ss.sensor_id = stu.sensor_id
            WHERE stu.user_id = %s
            ORDER BY d.device_name, st.name
        """, (user_id,))
        return cur.fetchall()
    finally:
        cur.close(); conn.close()


def delete_device(device_id: int, user_id: int):
    """
    Elimina il device dell'utente.
    Nota: per FK ON DELETE CASCADE verranno eliminate anche le righe su sensor_to_user.
    """
    conn = connection(); cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM device
             WHERE device_id = %s AND user_id = %s
             RETURNING device_id
        """, (device_id, user_id))
        ok = cur.fetchone()
        conn.commit()
        return (ok is not None, "Dispositivo eliminato." if ok else "Device non trovato o non tuo.")
    except Exception as e:
        conn.rollback(); return False, f"Errore: {e}"
    finally:
        cur.close(); conn.close()


def rename_device(device_id: int, user_id: int, new_name: str):
    conn = connection(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE device
               SET device_name = %s
             WHERE device_id = %s AND user_id = %s
             RETURNING device_id
        """, (new_name.strip(), device_id, user_id))
        ok = cur.fetchone()
        conn.commit()
        return (ok is not None, "Nome dispositivo aggiornato." if ok else "Device non trovato o non tuo.")
    except Exception as e:
        conn.rollback(); return False, f"Errore: {e}"
    finally:
        cur.close(); conn.close()

def list_user_devices_detailed(user_id: int):
    """
    Ritorna [(device_id, device_name, device_type_name, registered_at)] dell'utente.
    """
    conn = connection(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT d.device_id, d.device_name, COALESCE(dt.name,'-') AS device_type_name, d.registered_at
            FROM device d
            LEFT JOIN device_type dt ON dt.device_type_id = d.device_type_id
            WHERE d.user_id = %s
                AND COALESCE(dt.name, '') <> 'VirtualDevice'   -- nasconde i device virtuali
                AND d.device_name <> 'SingleSensor'
            ORDER BY d.registered_at DESC, d.device_id DESC
        """, (user_id,))
        return cur.fetchall()
    finally:
        cur.close(); conn.close()

def toggle_sensor_status(sensor_id: int, user_id: int) -> tuple[bool, str, bool]:
    """
    Attiva/disattiva un sensore per l'utente.
    Se non esiste ancora in sensor_status viene creato.
    Ritorna (ok, msg, new_state).
    """
    conn = connection(); cur = conn.cursor()
    try:
        # controlla che il sensore appartenga all'utente
        cur.execute("SELECT 1 FROM sensor_to_user WHERE sensor_id = %s AND user_id = %s", (sensor_id, user_id))
        if not cur.fetchone():
            return False, "Sensore non trovato o non appartiene all'utente.", False

        # recupera stato attuale
        cur.execute("SELECT active FROM sensor_status WHERE sensor_id = %s", (sensor_id,))
        row = cur.fetchone()

        if row is None:
            # se non c'è riga -> creiamo e impostiamo TRUE
            cur.execute(
                "INSERT INTO sensor_status (sensor_id, active) VALUES (%s, TRUE)",
                (sensor_id,)
            )
            new_state = True
        else:
            # se esiste -> toggliamo
            current_state = row[0]
            new_state = not current_state
            cur.execute(
                "UPDATE sensor_status SET active = %s, updated_at = NOW() WHERE sensor_id = %s",
                (new_state, sensor_id)
            )

        conn.commit()
        msg = f"Sensore {sensor_id} {'attivato' if new_state else 'disattivato'}."
        return True, msg, new_state

    except Exception as e:
        conn.rollback()
        return False, f"Errore: {e}", False
    finally:
        cur.close(); conn.close()

# utility/database_connection.py  —— PATCH

import psycopg

def _get_recommendations(user_id: int, domain: str):
    """
    Legge dai rankings user_{domain}_rankings e filtra con {domain}_recommendation_feedback.
    Mostra solo is_positive=1 (default) e ordina per rank (poi success_prob).
    """
    rankings_tbl = f"user_{domain}_rankings"
    feedback_tbl = f"{domain}_recommendation_feedback"

    conn = connection()
    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    try:
        sql = f"""
            SELECT
                r.recommendation_id,
                COALESCE(r.title, '')        AS title,
                COALESCE(r.description, '')  AS description,
                COALESCE(r.success_prob, 1.0) AS success_prob,
                r.rank
            FROM {rankings_tbl} AS r
            LEFT JOIN {feedback_tbl} AS fb
              ON fb.user_id = r.user_id
             AND fb.recommendation_id = r.recommendation_id
            WHERE r.user_id = %s
              AND COALESCE(fb.is_positive, 1) = 1
            ORDER BY r.rank ASC, r.success_prob DESC;
        """
        cur.execute(sql, (user_id,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


# alias usati dal frontend
def get_available_recommendations(user_id: int):
    return _get_recommendations(user_id, "workout")

def get_available_nutrition_recommendations(user_id: int):
    return _get_recommendations(user_id, "nutrition")


# ---------- FEEDBACK (no blacklist) ----------
def _log_feedback(user_id: int, recommendation_id: str, domain: str, is_positive: bool, comment: str | None = None):
    """
    Upsert in {domain}_recommendation_feedback: 1=mostra, 0=nascondi.
    (recommendation_id è TEXT ora!)
    """
    table = f"{domain}_recommendation_feedback"
    conn = connection()
    cur = conn.cursor()
    try:
        sql = f"""
            INSERT INTO {table} (user_id, recommendation_id, is_positive, noted_at, comment)
            VALUES (%s, %s, %s, NOW(), %s)
            ON CONFLICT (user_id, recommendation_id)
            DO UPDATE SET
                is_positive = EXCLUDED.is_positive,
                noted_at    = NOW(),
                comment     = COALESCE(EXCLUDED.comment, {table}.comment);
        """
        cur.execute(sql, (user_id, str(recommendation_id), 1 if is_positive else 0, comment))
        conn.commit()
    finally:
        cur.close()
        conn.close()

# workout
def log_positive_feedback(user_id: int, recommendation_id: str, comment: str | None = None):
    _log_feedback(user_id, recommendation_id, "workout", True, comment)

def log_negative_feedback(user_id: int, recommendation_id: str, comment: str | None = None):
    _log_feedback(user_id, recommendation_id, "workout", False, comment)

# nutrition
def log_positive_nutrition_feedback(user_id: int, recommendation_id: str, comment: str | None = None):
    _log_feedback(user_id, recommendation_id, "nutrition", True, comment)

def log_negative_nutrition_feedback(user_id: int, recommendation_id: str, comment: str | None = None):
    _log_feedback(user_id, recommendation_id, "nutrition", False, comment)

# ---------- RESET (rimette tutto visibile) ----------
def _reset_user_history(user_id: int, domain: str) -> bool:
    table = f"{domain}_recommendation_feedback"
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {table} SET is_positive = 1, noted_at = NOW() WHERE user_id = %s;", (user_id,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def reset_user_workout_preferences(user_id: int) -> bool:
    return _reset_user_history(user_id, "workout")

def reset_user_nutrition_preferences(user_id: int) -> bool:
    return _reset_user_history(user_id, "nutrition")
