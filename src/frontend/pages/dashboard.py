# pages/dashboard.py
import streamlit as st
from utility import database_connection as db
from frontend_utility import ui

# Page config
st.set_page_config(page_title="Dashboard", layout="wide", initial_sidebar_state="collapsed")
ui.load_css()

# Auth gate
if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    st.warning("Effettua il login per accedere.")
    st.stop()

user_id = st.session_state["user_id"]
name, surname = db.retrive_name(user_id)

# Layout colonne
sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)  # <-- comune

with main_col:
    ui.render_header(f"Benvenuto, {name} {surname}!", "La tua dashboard personale")  # <-- comune

    # Oggi
    st.markdown("#### Oggi")

    r1c1, r1c2 = st.columns(2)
    with r1c1:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Passi", "8,214", "+5%")
        st.markdown('</div>', unsafe_allow_html=True)
    with r1c2:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("HR medio", "76 bpm", "-2")
        st.markdown('</div>', unsafe_allow_html=True)

    r2c1, r2c2 = st.columns(2)
    with r2c1:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Sonno", "7h 18m", "+24m")
        st.markdown('</div>', unsafe_allow_html=True)
    with r2c2:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Calorie", "1,932 kcal", "≈")
        st.markdown('</div>', unsafe_allow_html=True)

    st.divider()
    st.markdown("#### Ultime attività")
    st.info("Qui puoi mostrare gli ultimi pasti inviati, l’ultimo workout, ecc.")
