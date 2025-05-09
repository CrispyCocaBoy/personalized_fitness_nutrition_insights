import time

import streamlit as st
import psycopg2
import bcrypt

# Migliore le impostazioni del dispositivo
# Dare la possibilità di poterlo non vedere

# === Connessione DB ===
def connection():
    return psycopg2.connect(host="postgres", dbname="user_device_db", user="admin", password="admin")

# === Info utente ===
def get_user_info(user_id):
    conn = connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.username, u.email, up.name, up.surname
        FROM users u
        LEFT JOIN users_profile up ON u.user_id = up.user_id
        WHERE u.user_id = %s
    """, (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row if row else ("", "", "", "")

# === Hash password attuale ===
def get_password_hash(user_id):
    conn = connection()
    cur = conn.cursor()
    cur.execute("SELECT password FROM users WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

# === Aggiorna info utente ===
def update_user_info(user_id, username, hashed_password, name, surname):
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE users SET username = %s, password = %s
            WHERE user_id = %s
        """, (username, hashed_password, user_id))

        cur.execute("SELECT 1 FROM users_profile WHERE user_id = %s", (user_id,))
        if cur.fetchone():
            cur.execute("""
                UPDATE users_profile SET name = %s, surname = %s, updated_at = NOW()
                WHERE user_id = %s
            """, (name, surname, user_id))
        else:
            cur.execute("""
                INSERT INTO users_profile (user_id, name, surname)
                VALUES (%s, %s, %s)
            """, (user_id, name, surname))

        conn.commit()
        st.success("Informazioni aggiornate con successo.")
    except Exception as e:
        conn.rollback()
        st.error(f"Errore durante l'aggiornamento: {e}")
    finally:
        cur.close()
        conn.close()

# === Ottieni lista orologi dal DB ===
def fetch_device_names():
    try:
        conn = connection()
        cur = conn.cursor()
        cur.execute("SELECT name FROM device_type ORDER BY name;")
        results = cur.fetchall()
        cur.close()
        conn.close()
        return [row[0] for row in results]
    except Exception as e:
        st.error(f"Errore nella connessione al database: {e}")
        return []

# === UI ===
if st.button("🏠"):
    st.switch_page("pages/dashboard.py")

st.title("Settings")

if "user_id" not in st.session_state:
    st.warning("Devi effettuare il login prima di accedere alle impostazioni.")
    st.stop()

user_id = st.session_state["user_id"]

# --- Sezione Profilo ---
st.subheader("Profilo Utente")

username, email, name, surname = get_user_info(user_id)

new_username = st.text_input("Username", value=username)
new_name = st.text_input("Nome", value=name)
new_surname = st.text_input("Cognome", value=surname)

st.markdown("#### Cambio password")
old_password = st.text_input("Vecchia Password", type="password")
new_password = st.text_input("Nuova Password", type="password")

if st.button("Salva modifiche"):
    if not all([new_username, old_password, new_password]):
        st.warning("Tutti i campi sono obbligatori per aggiornare la password.")
    else:
        current_hash = get_password_hash(user_id)
        if not current_hash or not bcrypt.checkpw(old_password.encode('utf-8'), current_hash.encode('utf-8')):
            st.error("La vecchia password non è corretta.")
        else:
            new_hashed = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            update_user_info(user_id, new_username, new_hashed, new_name, new_surname)

if st.button("Log out"):
    del st.session_state["user_id"]
    st.success("Logout effettuato.")
    time.sleep(1)
    st.switch_page("app.py")
    st.rerun()

# --- Sezione Dispositivo ---
st.subheader("Dispositivo")

watch_list = fetch_device_names()
selected_watch = st.selectbox("Seleziona il tuo orologio:", watch_list)

if st.button("Conferma orologio"):
    st.success(f"Hai selezionato: {selected_watch}")


