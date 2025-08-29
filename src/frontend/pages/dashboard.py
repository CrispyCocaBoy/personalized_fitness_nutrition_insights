# pages/dashboard.py
import streamlit as st
import requests
from datetime import datetime
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

# Gateway configuration - coerente con health.py
GATEWAY_BASE_URL = "http://gateway:8000"
today_date = datetime.now().strftime('%Y-%m-%d')

def fetch_gateway_data(endpoint, params=None):
    """Fetch data from gateway endpoints with error handling"""
    try:
        response = requests.get(f"{GATEWAY_BASE_URL}{endpoint}", params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            # silenzioso: non disturbare l'interfaccia
            return []
    except requests.exceptions.RequestException:
        # Fallback silenzioso
        return []

# Layout colonne
sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

with main_col:
    ui.render_header(f"Benvenuto, {name} {surname}!", "La tua dashboard personale")

    # Oggi - KPI reali dal gateway
    st.markdown("#### Oggi")

    # Fetch today's metrics from gateway
    metrics_today = fetch_gateway_data("/metrics/daily", {
        "user_id": user_id,
        "start_date": today_date,
        "end_date": today_date,
        "limit": 1
    })

    # Fetch today's meals from gateway
    meals_today = fetch_gateway_data("/meals/daily", {
        "user_id": user_id,
        "start_date": today_date,
        "end_date": today_date,
        "limit": 1
    })

    # Estrazione sicura con default a 0 se liste vuote/campi mancanti
    steps = int(metrics_today[0].get('steps_total', 0)) if metrics_today else 0
    hr_avg = int(metrics_today[0].get('hr_bpm_avg', 0)) if metrics_today else 0
    calories_burned = int(metrics_today[0].get('calories_total', 0)) if metrics_today else 0
    kcal_in = int(meals_today[0].get('kcal_total', 0)) if meals_today else 0
    balance = kcal_in - calories_burned

    # --- KPI cards riga 1: 5 colonne ---
    r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns(5)

    with r1c1:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Passi", f"{steps:,}", "Oggi")
        st.markdown('</div>', unsafe_allow_html=True)

    with r1c2:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("HR medio", f"{hr_avg} bpm", "Oggi")
        st.markdown('</div>', unsafe_allow_html=True)

    with r1c3:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Calorie immesse", f"{kcal_in:,} kcal")
        st.markdown('</div>', unsafe_allow_html=True)

    with r1c4:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Calorie bruciate", f"{calories_burned:,} kcal")
        st.markdown('</div>', unsafe_allow_html=True)

    with r1c5:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Bilancio", f"{balance:,} kcal")
        st.markdown('</div>', unsafe_allow_html=True)

    st.divider()
    st.markdown("#### Ultime attività")

    # Fetch recent activities from gateway
    recent_activities = fetch_gateway_data("/activities/facts", {
        "user_id": user_id,
        "limit": 5
    })

    if recent_activities:
        for activity in recent_activities:
            with st.expander(f"🏃 {activity.get('activity_name', 'N/A')} - {activity.get('start_ts', '')[:10]}"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Durata", f"{activity.get('duration_min', 0):.0f} min")
                with col2:
                    st.metric("Calorie", f"{activity.get('calories_total', 0)} kcal")
                with col3:
                    st.metric("Passi", f"{activity.get('steps_total', 0):,}")
    else:
        st.info("⏳ Nessuna attività recente registrata.")

    st.divider()
    st.markdown("#### Ultimi pasti")

    # Fetch recent meals from gateway
    recent_meals = fetch_gateway_data("/meals/facts", {
        "user_id": user_id,
        "limit": 5
    })

    if recent_meals:
        for meal in recent_meals:
            with st.expander(f"🍽️ {meal.get('meal_name', 'N/A')} - {meal.get('event_ts', '')[:10]}"):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Calorie", f"{meal.get('kcal', 0)} kcal")
                with col2:
                    st.metric("Carboidrati", f"{meal.get('carbs_g', 0)} g")
                with col3:
                    st.metric("Proteine", f"{meal.get('protein_g', 0)} g")
                with col4:
                    st.metric("Grassi", f"{meal.get('fat_g', 0)} g")
    else:
        st.info("⏳ Nessun pasto recente registrato.")
