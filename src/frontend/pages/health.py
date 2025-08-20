# pages/health.py
import time
from datetime import datetime

import streamlit as st
import pandas as pd
import numpy as np

from utility import database_connection as db
from frontend_utility import ui  # sidebar + header + css comuni

# =========================
# Config pagina + stile
# =========================
st.set_page_config(page_title="Health", layout="wide", initial_sidebar_state="collapsed")
ui.load_css()

# =========================
# Auth gate
# =========================
if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    st.warning("Effettua il login per accedere.")
    st.stop()

user_id = st.session_state["user_id"]
name, surname = db.retrive_name(user_id)

# =========================
# Layout colonne (convenzione di progetto)
# =========================
sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)  # <-- comune

with main_col:
    ui.render_header("Health", "Cardio: frequenza, variabilità e trend")  # <-- comune

    # ---- KPI cardiaci (placeholder) ----
    st.markdown("#### Oggi — Cardio")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("HR a riposo", "58 bpm", "+1")
        st.markdown("</div>", unsafe_allow_html=True)
    with k2:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("HR medio", "76 bpm", "-2")
        st.markdown("</div>", unsafe_allow_html=True)
    with k3:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("HR massimo", "142 bpm", "≈")
        st.markdown("</div>", unsafe_allow_html=True)
    with k4:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("HRV (RMSSD)", "52 ms", "+4")
        st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    # ---- Grafico BPM (placeholder) ----
    st.markdown("#### Andamento BPM (placeholder)")

    # Dati fittizi: letture ogni 5 minuti dalle 07:00 alle 22:55
    time_index = pd.date_range(start="07:00", end="22:55", freq="5min")
    # Serie HR con andamento plausibile (baseline + variazione dolce + rumore)
    baseline = 70 + 5*np.sin(np.linspace(0, 6*np.pi, len(time_index)))
    noise = np.random.normal(0, 2.5, len(time_index))
    bpm_values = np.clip(baseline + noise, 48, 160).round().astype(int)

    bpm_df = pd.DataFrame({"Ora": time_index, "BPM": bpm_values}).set_index("Ora")
    st.line_chart(bpm_df)

    st.divider()

    # ---- Ultime letture cardiache (placeholder) ----
    st.markdown("#### Ultime letture cardiache")
    # Mostra le ultime 10 letture come tabellina
    last_samples = bpm_df.tail(10).reset_index()
    last_samples.columns = ["Ora", "BPM"]
    st.dataframe(last_samples, hide_index=True, use_container_width=True)
