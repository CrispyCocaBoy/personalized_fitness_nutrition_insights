# pages/meals.py
import os
import uuid
from datetime import datetime
import streamlit as st
from utility import database_connection as db
from frontend_utility import ui  # sidebar + header + css comuni
import time

# =========================
# Config pagina + stile
# =========================
st.set_page_config(page_title="Pasti", layout="wide", initial_sidebar_state="collapsed")
ui.load_css()

# =========================
# Auth gate
# =========================
if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    st.warning("Effettua il login per accedere.")
    st.stop()

user_id = st.session_state["user_id"]
name, surname = db.retrive_name(user_id)
