import streamlit as st
import psycopg2
import bcrypt
from utility import database_connection as db

# Funzione per la verifica delle credenziali
def check_credentials(login_input, password, method):
    try:
        conn = db.connection()
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

# UI
st.set_page_config(page_title="Login", layout="centered")
# Bottone per tonrare alla home
if st.button("🚪 Inizia"):
        st.switch_page("app.py")

st.title("🔐 Accedi")

# Metodo di login: username o email
method = st.radio("Accedi con:", ["Username", "Email"])
login_input = st.text_input(method)  # Campo dinamico
password = st.text_input("Password", type="password")


# Bottone di login
if st.button("Accedi"):
    status, user_id = check_credentials(login_input, password, method)

    if status == "success":
        st.session_state["logged_in"] = True
        st.session_state["user_id"] = user_id
        st.success("Login riuscito! Reindirizzamento in corso...")
        st.switch_page("pages/dashboard.py")

    elif status == "not_found":
        st.error(f"Nessun account trovato con questo {method.lower()}.")


    elif status == "wrong_password":
        st.error("Password errata.")

    else:
        st.error("Errore imprevisto durante il login.")

if st.button("Se non hai un account clicca qui per registrarti"):
    st.switch_page("pages/signup.py")