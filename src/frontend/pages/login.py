# login_page.py
import streamlit as st
import bcrypt
from utility import database_connection as db

def check_credentials(username, password):
    conn = db.connection()
    cur = conn.cursor()
    cur.execute("SELECT password FROM users WHERE username = %s", (username,))
    result = cur.fetchone()
    conn.close()

    if result:
        hashed_pw = result[0].encode('utf-8')
        return bcrypt.checkpw(password.encode('utf-8'), hashed_pw)
    return False

# UI login
st.set_page_config(page_title="login", layout="centered")

if st.button("🚪 Inizia"):
        st.switch_page("app.py")

st.title("🔑 Accesso al servizio")

username = st.text_input("Username")
password = st.text_input("Password", type="password")
login_btn = st.button("Accedi")

if st.button("Se hai un account registrati qui"):
    st.switch_page("pages/signin.py")

if login_btn:
    if check_credentials(username, password):
        st.success("Login riuscito!")
        st.session_state["logged_in"] = True
        st.session_state["username"] = username
        st.switch_page("pages/dashboard.py")
    else:
        st.error("Username o password errati.")
