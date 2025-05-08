import psycopg2
import bcrypt

def connection():
    return psycopg2.connect(host="postgres", dbname="user_device_db", user="admin", password="admin")

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
def complete_profile(user_id, name, surname, gender, birthday):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users_profile (user_id, name, surname, gender, birthday)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, name, surname, gender, birthday))
        conn.commit()
        conn.close()
        return True, "Profilo completato con successo!"
    except Exception as e:
        conn.close()
        return False, f"Errore durante l'inserimento del profilo: {e}"

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
        conn.close()
        return True, "Altezza salvata con successo."
    except Exception as e:
        conn.close()
        return False, f"Errore altezza: {e}"

def set_weight(user_id, weight):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO weight (user_id, kg)
            VALUES (%s, %s)
        """, (user_id, weight))
        conn.commit()
        conn.close()
        return True, "Peso salvato con successo."
    except Exception as e:
        conn.close()
        return False, f"Errore peso: {e}"

# Login
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
        st.error(f"Errore di connessione al database: {e}")
        return "error", None

