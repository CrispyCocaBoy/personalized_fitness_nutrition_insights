# pages/health.py
from datetime import datetime, timedelta
from typing import Optional

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
# Backend adapter (HTTP)
# =========================
BACKEND_URL = "http://gateway:8000"

def _backend() -> Optional[str]:
    if not BACKEND_URL:
        return None
    return BACKEND_URL.rstrip("/")

def _iso_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def _today_utc_str() -> str:
    return _iso_date(datetime.utcnow())

# =========================
# Data loaders
# =========================
@st.cache_data(ttl=20, show_spinner=False)
def load_metrics_daily_day(user_id: str, day_str: str) -> pd.DataFrame:
    """
    Aggregati giornalieri per UN solo giorno (YYYY-MM-DD).
    """
    be = _backend()
    if not be:
        return pd.DataFrame()
    try:
        import requests
        params = {"user_id": str(user_id), "start_date": day_str, "end_date": day_str, "limit": 1}
        r = requests.get(f"{be}/metrics/daily", params=params, timeout=20)
        r.raise_for_status()
        rows = r.json() or []
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "event_date" in df.columns:
            df["event_date"] = pd.to_datetime(df["event_date"], utc=True, errors="coerce")
        for c in ["hr_bpm_avg", "spo2_avg", "steps_total", "calories_total", "windows_count"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "last_window_end" in df.columns:
            df["last_window_end"] = pd.to_datetime(df["last_window_end"], utc=True, errors="coerce")
        return df
    except Exception as e:
        st.warning(f"Errore nel caricamento del giorno {day_str}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=20, show_spinner=False)
def load_metrics_facts_day(user_id: str, day_str: str, limit: int = 5000) -> pd.DataFrame:
    """
    Letture minute-level (facts) del giorno selezionato.
    GET /metrics/facts?user_id=...&start_date=day&end_date=day
    """
    be = _backend()
    if not be:
        return pd.DataFrame()
    try:
        import requests
        params = {
            "user_id": str(user_id),
            "start_date": day_str,
            "end_date": day_str,
            "limit": int(limit),
        }
        r = requests.get(f"{be}/metrics/facts", params=params, timeout=30)
        r.raise_for_status()
        data = r.json() or []
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)

        # Parse ISO → datetime UTC
        for c in ("window_start", "window_end"):
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

        # Tipizza numeriche
        for c in ("hr_bpm", "spo2", "step_count", "calories"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # Ordina per tempo
        if "window_end" in df.columns:
            df = df.sort_values("window_end").reset_index(drop=True)

        # Alcune pipeline possono generare più righe per lo stesso minuto:
        # mediamo per minuto per avere una singola serie pulita.
        if "window_end" in df.columns:
            df_min = (
                df.set_index("window_end")
                  .groupby(pd.Grouper(freq="T"))
                  .agg({
                      "hr_bpm": "mean" if "hr_bpm" in df.columns else "first",
                      "spo2": "mean"   if "spo2" in df.columns   else "first",
                  })
            )
            df_min = df_min.dropna(how="all")
            df_min = df_min.reset_index()
            return df_min
        return df
    except Exception as e:
        st.warning(f"Errore facts del giorno {day_str}: {e}")
        return pd.DataFrame()

# =========================
# Layout colonne
# =========================
sidebar_col, main_col = st.columns([0.9, 6.1], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

    st.markdown("### Backend")
    st.caption(f"Gateway: {_backend()}")

    st.markdown("---")
    st.markdown("### Giorno (UTC)")
    selected_date = st.date_input(
        "Scegli il giorno",
        value=pd.to_datetime(_today_utc_str()).date(),
        format="YYYY-MM-DD"
    )
    day_str = _iso_date(pd.to_datetime(selected_date).to_pydatetime())

    st.markdown("---")
    st.markdown("### Visualizzazioni")
    show_hr   = st.checkbox("HR medio (intragiornaliero)", True)
    show_spo2 = st.checkbox("SpO₂ media (intragiornaliera)", True)

with main_col:
    ui.render_header("Health", "Sintesi cardio‑respiratoria — giorno selezionato")

    # ---------- KPI dal daily ----------
    daily = load_metrics_daily_day(str(user_id), day_str)
    if daily.empty:
        st.info(f"Nessun dato aggregato per il giorno {day_str}.")
        st.stop()
    row = daily.iloc[0]

    steps_sum = int(row.get("steps_total", 0) or 0)
    kcal_sum  = float(row.get("calories_total", 0.0) or 0.0)
    hr_avg    = float(row.get("hr_bpm_avg")) if pd.notna(row.get("hr_bpm_avg")) else np.nan
    spo2_avg  = float(row.get("spo2_avg")) if pd.notna(row.get("spo2_avg")) else np.nan

    st.markdown(f"#### Riepilogo — {day_str}")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("HR medio", f"{hr_avg:.0f} bpm" if not np.isnan(hr_avg) else "—")
        st.markdown("</div>", unsafe_allow_html=True)
    with k2:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("SpO₂ media", f"{spo2_avg:.1f} %" if not np.isnan(spo2_avg) else "—")
        st.markdown("</div>", unsafe_allow_html=True)
    with k3:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Passi (totale)", f"{steps_sum:,}".replace(",", "."))
        st.markdown("</div>", unsafe_allow_html=True)
    with k4:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.metric("Calorie (totale)", f"{kcal_sum:.1f}")
        st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    # ---------- Grafici intragiornalieri (facts del giorno) ----------
    facts_day = load_metrics_facts_day(str(user_id), day_str, limit=5000)

    if facts_day.empty or "window_end" not in facts_day.columns:
        st.info("Nessuna serie minuto/minuto disponibile per il giorno selezionato.")
    else:
        facts_day = facts_day.set_index("window_end")

        # smoothing leggero (rolling 5 minuti)
        if "hr_bpm" in facts_day.columns:
            facts_day["hr_bpm_smooth"] = facts_day["hr_bpm"].rolling(5, min_periods=1).mean()
        if "spo2" in facts_day.columns:
            facts_day["spo2_smooth"] = facts_day["spo2"].rolling(5, min_periods=1).mean()

        if show_hr and "hr_bpm_smooth" in facts_day.columns:
            st.markdown("#### Andamento HR (giornata)")
            st.line_chart(facts_day[["hr_bpm_smooth"]], use_container_width=True)
            st.caption("Serie minuto/minuto (media mobile 5'). Orari in UTC.")

        if show_spo2 and "spo2_smooth" in facts_day.columns:
            st.markdown("#### SpO₂ (giornata)")
            st.line_chart(facts_day[["spo2_smooth"]], use_container_width=True)
            st.caption("Serie minuto/minuto (media mobile 5'). Orari in UTC.")

    st.divider()

    # ---------- Dettaglio del giorno (daily) ----------
    st.markdown("#### Dettaglio (aggregati del giorno)")
    show_cols = [c for c in [
        "event_date", "hr_bpm_avg", "spo2_avg",
        "steps_total", "calories_total",
        "windows_count", "last_window_end"
    ] if c in daily.columns]
    st.dataframe(daily[show_cols], hide_index=True, use_container_width=True)
