# pages/dashboard.py
import streamlit as st
import requests
from datetime import datetime, timedelta
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

# Gateway configuration - corretta per essere coerente con health.py
GATEWAY_BASE_URL = "http://gateway:8000"  # Usa lo stesso URL di health.py
today_date = datetime.now().strftime('%Y-%m-%d')

def fetch_gateway_data(endpoint, params=None):
    """Fetch data from gateway endpoints with error handling"""
    try:
        response = requests.get(f"{GATEWAY_BASE_URL}{endpoint}", params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            # Mostra errore solo nel log, non all'utente per non disturbare l'interfaccia
            return []
    except requests.exceptions.RequestException:
        # Fallback silenzioso - non mostra errore all'utente
        return []

# Layout colonne
sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

with main_col:
    ui.render_header(f"Benvenuto, {name} {surname}!", "La tua dashboard personale")

    # --- NUOVO INVITO ALL'AZIONE ---
    st.info("💡 Visualizza i tuoi consigli personalizzati di allenamento e nutrizione nella nuova pagina dedicata!", icon="💡")
    if st.button("Vai ai Consigli del Giorno", use_container_width=True, type="primary"):
        st.switch_page("pages/recommendations.py")
    
    st.markdown("---")

    # Sezione KPI (rimane invariata)
    st.markdown("#### Riepilogo Ultimi Dati")
    latest_metrics = db.get_latest_daily_metrics(user_id)
    if not latest_metrics:
        st.info("Nessun dato giornaliero disponibile al momento.")
    else:
        # ... (il resto del codice per i KPI rimane identico)
        steps = int(latest_metrics.get("steps_total", 0))
        hr_avg = int(latest_metrics.get("hr_bpm_avg", 0))
        sleep_minutes = int(latest_metrics.get("sleep_total_minutes", 0))
        calories = int(latest_metrics.get("calories_total", 0))
        sleep_hours = sleep_minutes // 60
        sleep_rem_minutes = sleep_minutes % 60
        sleep_formatted = f"{sleep_hours}h {sleep_rem_minutes}m"
        r1c1, r1c2 = st.columns(2)
        with r1c1:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("Passi", f"{steps:,}".replace(",", "."))
            st.markdown('</div>', unsafe_allow_html=True)
        with r1c2:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("HR medio", f"{hr_avg} bpm")
            st.markdown('</div>', unsafe_allow_html=True)
        r2c1, r2c2 = st.columns(2)
        with r2c1:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("Sonno", sleep_formatted)
            st.markdown('</div>', unsafe_allow_html=True)
        with r2c2:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("Calorie", f"{calories} kcal")
            st.markdown('</div>', unsafe_allow_html=True)