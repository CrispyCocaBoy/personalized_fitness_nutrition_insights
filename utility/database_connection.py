import psycopg2
import bcrypt
import random
import string

# --- CONNESSIONI AI DATABASE ---

def connection():
    """Connessione al DB principale per utenti, profili, device, ecc."""
    return psycopg2.connect(
        host="cockroachdb",
        port=26257,
        dbname="user_device_db", # DB per i dati del frontend
        user="root"
    )

def connection_recommendations():
    """Connessione al DB per leggere le raccomandazioni generate da Spark."""
    return psycopg2.connect(
        host="cockroachdb",
        port=26257,
        dbname="defaultdb", # DB per i dati di Spark
        user="root"
    )

# --- FUNZIONI UTENTE (usano la connessione standard a 'user_device_db') ---

def register_user(username, email, password):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        if cur.fetchone():
            conn.close()
            return False, "Username già esistente.", None

        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            conn.close()
            return False, "Email già esistente.", None

        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cur.execute(
            "INSERT INTO users (username, email, password) VALUES (%s, %s, %s) RETURNING user_id",
            (username, email, hashed)
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        return True, "Registrazione avvenuta con successo.", user_id
    except Exception as e:
        return False, f"Errore durante la registrazione: {e}", None
    finally:
        cur.close()
        conn.close()

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
        cur.close()
        conn.close()

def set_height(user_id, height):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users_profile SET height = %s WHERE user_id = %s", (height, user_id))
        conn.commit()
        return True, "Altezza salvata con successo."
    except Exception as e:
        conn.rollback()
        return False, f"Errore altezza: {e}"
    finally:
        cur.close()
        conn.close()

def set_weight(user_id, weight):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO weight (user_id, weight, measured_at) VALUES (%s, %s, NOW())", (user_id, weight))
        conn.commit()
        return True, "Peso salvato con successo."
    except Exception as e:
        conn.rollback()
        return False, f"Errore peso: {e}"
    finally:
        cur.close()
        conn.close()

def check_credentials(login_input, password, method):
    conn = connection()
    cur = conn.cursor()
    try:
        query = "SELECT user_id, password FROM users WHERE "
        query += "username = %s" if method == "Username" else "email = %s"
        cur.execute(query, (login_input,))
        result = cur.fetchone()
        if not result:
            return "not_found", None
        user_id, hashed_pw = result[0], result[1].encode('utf-8')
        if bcrypt.checkpw(password.encode('utf-8'), hashed_pw):
            return "success", user_id
        else:
            return "wrong_password", None
    except Exception as e:
        print(f"Errore check_credentials: {e}")
        return "error", None
    finally:
        cur.close()
        conn.close()

def retrive_name(user_id):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name, surname FROM users_profile WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)
    except Exception as e:
        print(f"Errore recupero nome: {e}")
        return None, None
    finally:
        cur.close()
        conn.close()

def random_selection():
    conn = connection()
    cur = conn.cursor()
    cur.execute("SELECT device_type_id, name FROM device_type")
    available_types = cur.fetchall()
    conn.close()
    return random.choice(available_types)

def stream_selection():
    conn = connection()
    cur = conn.cursor()
    cur.execute("SELECT device_type_id, name FROM device_type")
    available_types = cur.fetchall()
    conn.close()
    return available_types

def bind_device(user_id, device_type_id, device_type_name):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM users_profile WHERE user_id = %s", (user_id,))
        user_name = cur.fetchone()
        device_name = f"{device_type_name} of {user_name[0]}"
        serial_number = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        cur.execute("""
                    INSERT INTO device (user_id, device_name, device_type_id, serial_number, registered_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING device_id
                    """, (user_id, device_name, device_type_id, serial_number))
        device_id = cur.fetchone()[0]
        cur.execute("""
                    SELECT sensor_type_id
                    FROM predefined_device_type_sensors
                    WHERE device_type_id = %s
                    """, (device_type_id,))
        sensors_to_generate = [row[0] for row in cur.fetchall()]
        for sensor_type_id in sensors_to_generate:
            cur.execute("""
                        INSERT INTO sensor_to_user (device_id, user_id, sensor_type_id, created_at)
                        VALUES (%s, %s, %s, NOW())
                        """, (device_id, user_id, sensor_type_id))
        conn.commit()
        return True, f"Device {device_name} con sensori {sensors_to_generate} creato con successo!"
    except Exception as e:
        conn.rollback()
        return False, f"Errore: {e}"
    finally:
        conn.close()

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
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return [dict(zip(colnames, row)) for row in rows]

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
        user_id, (food.get("name") or "").strip(), food.get("quantity"),
        (food.get("unit") or None), int(food.get("calories") or 0),
        int(food.get("carbohydrates") or 0), int(food.get("protein") or 0),
        int(food.get("fat") or 0), int(food.get("fiber") or 0),
        int(food.get("sugars") or 0), int(food.get("saturated_fat") or 0),
        int(food.get("trans_fat") or 0), int(food.get("cholesterol") or 0),
        int(food.get("potassium") or 0), int(food.get("iron") or 0),
        int(food.get("vitamin_c") or 0), int(food.get("vitamin_a") or 0),
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

def get_workout_recommendations(user_id: str, limit: int = 5) -> list[dict]:
    """
    Recupera le ultime raccomandazioni di allenamento non completate per un utente.
    """
    conn = connection_recommendations() # <-- USA LA CONNESSIONE A 'defaultdb'
    cur = conn.cursor()
    try:
        # NOTA: Questa query potrebbe fallire se la tabella 'user_workout_recommendations'
        # e la colonna 'status' non esistono in 'defaultdb'.
        query = """
            SELECT recommendation_id, user_id, workout_type, details, generated_at
            FROM user_workout_recommendations
            WHERE CAST(user_id AS INT) = %s AND status = 'pending'
            ORDER BY generated_at DESC
            LIMIT %s;
        """
        cur.execute(query, (int(user_id), limit))
        recommendations = []
        columns = [desc[0] for desc in cur.description]
        for row in cur.fetchall():
            recommendations.append(dict(zip(columns, row)))
        return recommendations
    except Exception as e:
        print(f"!!! ERRORE DURANTE IL RECUPERO DELLE RACCOMANDAZIONI: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def get_latest_daily_metrics(user_id: str) -> dict | None:
    conn = connection()
    cur = conn.cursor()
    try:
        # NOTA: Questa query potrebbe fallire se 'gold_metrics_daily' non è in 'user_device_db'
        query = """
            SELECT 
                steps_total,
                hr_bpm_avg,
                sleep_total_minutes,
                calories_total
            FROM gold_metrics_daily
            WHERE user_id = %s
            ORDER BY event_date DESC
            LIMIT 1;
        """
        cur.execute(query, (int(user_id),))
        row = cur.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))
    except Exception as e:
        print(f"ERROR fetching latest daily metrics: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def get_next_recommendation(user_id: str) -> dict | None:
    """
    Recupera la prossima raccomandazione valida per un utente,
    escludendo quelle presenti nella sua blacklist e usando la data del database.
    """
    conn = connection_recommendations() # <-- USA LA CONNESSIONE A 'defaultdb'
    cur = conn.cursor()
    try:
        query = """
            SELECT r.recommendation_id, r.workout_type, r.details
            FROM user_workout_rankings AS r
            LEFT JOIN user_device_db.public.user_recommendation_blacklist AS b
            ON r.user_id = b.user_id AND r.recommendation_id = b.recommendation_id
            WHERE r.user_id = %s
              AND b.blacklist_id IS NULL -- Esclude le raccomandazioni in blacklist
              AND r.generated_at::date = CURRENT_DATE -- Considera solo le classifiche di oggi (robusto)
            ORDER BY r.rank ASC
            LIMIT 1;
        """
        cur.execute(query, (int(user_id),))
        row = cur.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))
    except Exception as e:
        print(f"ERROR fetching next recommendation: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def add_to_blacklist(user_id: str, recommendation_id: int):
    """Aggiunge una raccomandazione alla blacklist di un utente (in 'user_device_db')."""
    conn = connection() # <-- Usa la connessione standard
    cur = conn.cursor()
    try:
        query = "INSERT INTO user_recommendation_blacklist (user_id, recommendation_id) VALUES (%s, %s);"
        cur.execute(query, (int(user_id), recommendation_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"ERROR adding to blacklist: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def log_positive_feedback(user_id: str, recommendation_id: int):
    """Registra un feedback positivo per una raccomandazione (in 'user_device_db')."""
    conn = connection() # <-- Usa la connessione standard
    cur = conn.cursor()
    try:
        query = "INSERT INTO user_recommendation_feedback (user_id, recommendation_id) VALUES (%s, %s);"
        cur.execute(query, (int(user_id), recommendation_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"ERROR logging positive feedback: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def get_available_recommendations(user_id: str) -> list[dict]:
    """
    Recupera TUTTE le raccomandazioni valide per un utente (non in blacklist),
    complete di probabilità di successo per la selezione pesata.
    """
    conn = connection_recommendations() # <-- USA LA CONNESSIONE A 'defaultdb'
    cur = conn.cursor()
    try:
        query = """
            SELECT r.recommendation_id, r.workout_type, r.details, r.success_prob
            FROM user_workout_rankings AS r
            WHERE r.user_id = %s
              AND r.generated_at::date = CURRENT_DATE
              AND NOT EXISTS (
                  SELECT 1
                  FROM user_device_db.public.user_recommendation_blacklist b
                  WHERE b.user_id = r.user_id
                    AND b.recommendation_id = r.recommendation_id
              );
        """
        cur.execute(query, (int(user_id),))
        rows = cur.fetchall()
        if not rows:
            return []
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"ERROR fetching available recommendations: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def reset_blacklist(user_id: str) -> bool:
    """Rimuove tutte le voci dalla blacklist per un dato utente (in 'user_device_db')."""
    conn = connection() # <-- Usa la connessione standard
    cur = conn.cursor()
    try:
        query = "DELETE FROM user_recommendation_blacklist WHERE user_id = %s;"
        cur.execute(query, (int(user_id),))
        conn.commit()
        return True
    except Exception as e:
        print(f"ERROR resetting blacklist: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()


# Aggiungi queste nuove funzioni in database_connection.py

def get_available_nutrition_recommendations(user_id: str) -> list[dict]:
    """
    Recupera TUTTE le raccomandazioni nutrizionali valide per un utente (non in blacklist),
    complete di probabilità di successo per la selezione pesata.
    """
    conn = connection_recommendations() # <-- Connessione a 'defaultdb'
    cur = conn.cursor()
    try:
        # Query sulla tabella di ranking nutrizionale, escludendo la blacklist nutrizionale
        query = """
            SELECT r.id_recommendation, r.nutrition_plan, r.details, r.success_prob
            FROM user_nutrition_rankings AS r
            WHERE r.user_id = %s
              AND r.generated_at::date = CURRENT_DATE
              AND NOT EXISTS (
                  SELECT 1
                  FROM user_device_db.public.user_nutrition_blacklist b
                  WHERE b.user_id = r.user_id
                    AND b.recommendation_id = r.id_recommendation
              );
        """
        cur.execute(query, (int(user_id),))
        rows = cur.fetchall()
        if not rows:
            return []
        
        # Rinominiamo le colonne per essere compatibili con la UI card generica
        # 'id_recommendation' -> 'recommendation_id'
        # 'nutrition_plan' -> 'workout_type'
        columns = ['recommendation_id', 'workout_type', 'details', 'success_prob']
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"ERROR fetching available nutrition recommendations: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def add_to_nutrition_blacklist(user_id: str, recommendation_id: int):
    """Aggiunge una raccomandazione nutrizionale alla blacklist di un utente."""
    conn = connection() # <-- Connessione a 'user_device_db'
    cur = conn.cursor()
    try:
        query = "INSERT INTO user_nutrition_blacklist (user_id, recommendation_id) VALUES (%s, %s);"
        cur.execute(query, (int(user_id), recommendation_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"ERROR adding to nutrition blacklist: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def log_positive_nutrition_feedback(user_id: str, recommendation_id: int):
    """Registra un feedback positivo per una raccomandazione nutrizionale."""
    conn = connection() # <-- Connessione a 'user_device_db'
    cur = conn.cursor()
    try:
        query = "INSERT INTO user_nutrition_feedback (user_id, recommendation_id) VALUES (%s, %s);"
        cur.execute(query, (int(user_id), recommendation_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"ERROR logging positive nutrition feedback: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def reset_nutrition_blacklist(user_id: str) -> bool:
    """Rimuove tutte le voci dalla blacklist nutrizionale per un dato utente."""
    conn = connection() # <-- Connessione a 'user_device_db'
    cur = conn.cursor()
    try:
        query = "DELETE FROM user_nutrition_blacklist WHERE user_id = %s;"
        cur.execute(query, (int(user_id),))
        conn.commit()
        return True
    except Exception as e:
        print(f"ERROR resetting nutrition blacklist: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()