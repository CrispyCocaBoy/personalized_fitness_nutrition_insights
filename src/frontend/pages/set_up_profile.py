import streamlit as st
import datetime
import time
from utility import database_connection as db


def complete_profile(user_id, name, surname, gender, birthday):
    conn = db.connection()
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


# Set UI
st.set_page_config(page_title="Completamento profilo", layout="centered")

if "user_id" not in st.session_state:
    st.error("Errore: utente non autenticato.")
    st.stop()

user_id = st.session_state["user_id"]

st.title("👤 Completa il tuo profilo")

with st.form("profile_form"):
    name = st.text_input("Nome")
    surname = st.text_input("Cognome")
    gender = st.selectbox("Genere", options=["Male", "Female", "Other"])
    birthday = st.date_input(
        "Data di nascita",
        min_value=datetime.date(1900, 1, 1),
        max_value=datetime.date.today(),
        value=datetime.date(2000, 1, 1),  # valore di default
        format="DD/MM/YYYY")

    submitted = st.form_submit_button("Salva profilo")

    if submitted:
        if not name or not surname:
            st.error("Compila tutti i campi.")
        else:
            success, msg = complete_profile(user_id, name, surname, gender, birthday)
            if success == True:
                st.success(msg)
                st.info("Ora verrai reindirizzato per completare il profilo.")
                time.sleep(1)
                st.switch_page("pages/set_up_profile_pt2.py")
            else:
                st.error(msg)

