import time
import os
import uuid
from datetime import datetime

import streamlit as st
import bcrypt

from utility import database_connection as db
from frontend_utility import ui

# =========================
# Config pagina + stile
# =========================
st.set_page_config(page_title="Settings", layout="wide", initial_sidebar_state="collapsed")
ui.load_css()

# =========================
# Auth gate
# =========================
if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    st.warning("Effettua il login per accedere.")
    st.stop()

user_id = st.session_state["user_id"]
name, surname = db.retrive_name(user_id)

# === Info utente ===
def get_user_info(user_id):
    conn = db.connection()
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
    conn = db.connection()
    cur = conn.cursor()
    cur.execute("SELECT password FROM users WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

# === Aggiorna info utente ===
def update_user_info(user_id, username, hashed_password, name, surname):
    conn = db.connection()
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
        conn = db.connection()
        cur = conn.cursor()
        cur.execute("SELECT name FROM device_type ORDER BY name;")
        results = cur.fetchall()
        cur.close()
        conn.close()
        return [row[0] for row in results]
    except Exception as e:
        st.error(f"Errore nella connessione al database: {e}")
        return []

# =========================
# Layout colonne (convenzione di progetto)
# =========================
sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

with main_col:
    # Header comune
    ui.render_header("Settings", "Gestisci profilo, password e dispositivo")

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

    # Logout
    if st.button("Log out"):
        del st.session_state["user_id"]
        st.success("Logout effettuato.")
        time.sleep(1)
        st.switch_page("app.py")
        st.rerun()

    st.divider()

    # --- Sezione Dispositivo ---
    st.subheader("Dispositivo")

    watch_list = fetch_device_names()
    selected_watch = st.selectbox("Seleziona il tuo orologio:", watch_list)

    if st.button("Conferma orologio"):
        st.success(f"Hai selezionato: {selected_watch}")
