# Chiede peso e altezza

import streamlit as st
import bcrypt
from utility import database_connection as db
import datetime
import time

# Funzione per aggiornare l'altezza
def set_height(user_id, height):
    conn = db.connection()
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

# Funzione per inserire il peso
def set_weight(user_id, weight):
    conn = db.connection()
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

# UI
st.set_page_config(page_title="Set del peso e dell'altezza", layout="centered")

if "user_id" not in st.session_state:
    st.error("Errore: utente non autenticato.")
    st.stop()

user_id = st.session_state["user_id"]

st.title("👤 Completa il tuo profilo")

with st.form("profile_form"):
    height = st.selectbox("Altezza (cm)", options=list(range(140, 211)))
    weight = st.selectbox("Peso (kg)", options=list(range(40, 151)))

    submitted = st.form_submit_button("Salva profilo")

    if submitted:
        success_h, msg_h = set_height(user_id, height)
        success_w, msg_w = set_weight(user_id, weight)

        if success_h and success_w:
            st.success("Profilo completato con successo!")
            time.sleep(1)
            st.session_state["logged_in"] = True
            st.switch_page("pages/dashboard.py")
        else:
            if not success_h:
                st.error(msg_h)
            if not success_w:
                st.error(msg_w)
