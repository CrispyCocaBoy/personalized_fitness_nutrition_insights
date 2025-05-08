import streamlit as st

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