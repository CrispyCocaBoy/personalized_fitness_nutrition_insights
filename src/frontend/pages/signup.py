import streamlit as st
import time
from utility import database_connection as db

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
            success, message, user_id = db.register_user(username, email, password)
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