import streamlit as st

# Il futuro per questa scheda è fare in modo che si crei un mini database intenro con tutti
# i dati relativi all'utente


if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    st.warning("Effettua il login per accedere.")
    st.stop()

st.title(f"Benvenuto, {st.session_state['user_id']}!")

if st.button("⚙️"):
    st.switch_page("pages/settings.py")