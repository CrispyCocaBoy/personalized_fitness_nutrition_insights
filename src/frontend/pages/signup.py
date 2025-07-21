import streamlit as st
import time
from utility import database_connection as db
import bcrypt


def register_user(username, email, password):
    conn = db.connection()
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

# UI registrazione
st.set_page_config(page_title="Registrazione", layout="centered")

if st.button("🚪 Inizia"):
        st.switch_page("app.py")

st.title("📝 Crea un nuovo account")


with st.form("signup_form"):
    username = st.text_input("Scegli uno username")
    email = st.text_input("Email")
    password = st.text_input("Scegli una password", type="password")
    password_confirm = st.text_input("Conferma password", type="password")
    submitted = st.form_submit_button("Registrati")

    if submitted:
        if password != password_confirm:
            st.error("Le password non coincidono.")
        elif not username or not password:
            st.error("Username e password obbligatori.")
        else:
            success, message, user_id = register_user(username, email, password)
            if success:
                st.success(message)
                st.session_state["user_id"] = user_id
                st.info("Ora verrai reindirizzato per completare il profilo.")
                time.sleep(1)
                st.switch_page("pages/set_up_profile.py")
            else:
                st.error(message)

if st.button("se hai già un account entra con login"):
    st.switch_page("pages/login.py")