import streamlit as st

st.set_page_config(page_title="Benvenuto", page_icon="🏋️", layout="centered")

st.title("Benvenuto nel Fitness and Nutrition app")
st.subheader("Accedi o Registrati per iniziare")

col1, col2 = st.columns(2)

with col1:
    if st.button("🔐 Accedi"):
        st.switch_page("pages/login.py")

with col2:
    if st.button("🆕 Registrati"):
        st.switch_page("pages/signup.py")
