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


def bind_device(user_id, device_type_id, device_type_name):
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

# Retrive default_meal
def default_food():
    conn = connection()  # tua funzione che apre la connessione
    cur = conn.cursor()

    cur.execute("SELECT name FROM default_foods ORDER BY name;")
    rows = cur.fetchall()

    # chiudi connessione
    cur.close()
    conn.close()

    # estrai i soli nomi
    return [r[0] for r in rows]

def personalized_food(user_id: str):
    conn = connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM user_foods WHERE user_id = %s ORDER BY name;",
        (user_id,)   # tupla!
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [r[0] for r in rows]


import uuid

def insert_personalized_food(user_id: str, food: dict):
    """
    Inserisce una riga in users_food.
    Colonne richieste presenti in 'food':
      name, quantity, unit, calories, carbohydrates, protein, fat, fiber, sugars,
      saturated_fat, trans_fat, cholesterol, potassium, iron, vitamin_c, vitamin_a, category
    I campi mancanti vengono inseriti come NULL/0 dove ha senso.
    Ritorna lo user_food_id (UUID) inserito.
    """
    from utility.database_connection import connection

    new_id = str(uuid.uuid4())

    sql = """
        INSERT INTO user_foods (
            user_food_id,
            user_id,
            name,
            quantity,
            unit,
            calories,
            carbohydrates,
            protein,
            fat,
            fiber,
            sugars,
            saturated_fat,
            trans_fat,
            cholesterol,
            potassium,
            iron,
            vitamin_c,
            vitamin_a,
            category
        ) VALUES (
            %s,  -- user_food_id
            %s,  -- user_id
            %s,  -- name
            %s,  -- quantity
            %s,  -- unit
            %s,  -- calories
            %s,  -- carbohydrates
            %s,  -- protein
            %s,  -- fat
            %s,  -- fiber
            %s,  -- sugars
            %s,  -- saturated_fat
            %s,  -- trans_fat
            %s,  -- cholesterol
            %s,  -- potassium
            %s,  -- iron
            %s,  -- vitamin_c
            %s,  -- vitamin_a
            %s   -- category
        )
        RETURNING user_food_id;
    """

    params = (
        new_id,
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


