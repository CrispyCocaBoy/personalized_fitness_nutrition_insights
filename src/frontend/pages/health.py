# pages/health.py
from datetime import datetime, timedelta
from typing import Optional, Tuple

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
# Backend adapter (HTTP) — usato solo internamente
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

        # Mediana per minuto per serie pulita
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

@st.cache_data(ttl=60, show_spinner=False)
def load_metrics_daily_range(user_id: str, start_date: str, end_date: str, limit: int = 20000) -> pd.DataFrame:
    """
    Carica la tabella daily per un intervallo [start_date, end_date].
    """
    be = _backend()
    if not be:
        return pd.DataFrame()
    try:
        import requests
        params = {
            "user_id": str(user_id),
            "start_date": start_date,
            "end_date": end_date,
            "limit": int(limit),
        }
        r = requests.get(f"{be}/metrics/daily", params=params, timeout=30)
        r.raise_for_status()
        rows = r.json() or []
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "event_date" in df.columns:
            df["event_date"] = pd.to_datetime(df["event_date"], utc=True, errors="coerce").dt.normalize()
        # Tipizza campi numerici
        for c in ["hr_bpm_avg", "spo2_avg", "steps_total", "calories_total", "windows_count"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.sort_values("event_date").reset_index(drop=True)
    except Exception as e:
        st.warning(f"Errore nel caricamento intervallo {start_date} → {end_date}: {e}")
        return pd.DataFrame()

# =========================
# Helpers per range mese/anno
# =========================
def month_bounds(d: datetime) -> Tuple[str, str]:
    first = d.replace(day=1)
    # prossimo mese: trucchino coi 32 giorni
    next_month = (first + timedelta(days=32)).replace(day=1)
    last = next_month - timedelta(days=1)
    return _iso_date(first), _iso_date(last)

def year_bounds(year: int) -> Tuple[str, str]:
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)
    return _iso_date(start), _iso_date(end)

# =========================
# Layout
# =========================
sidebar_col, main_col = st.columns([0.9, 6.1], gap="large")

with sidebar_col:
    # Solo profilo / menu — niente più selettori data o gateway qui
    ui.render_sidebar(name, surname, user_id)

with main_col:
    ui.render_header("Health", "Sintesi cardio‑respiratoria")

    # ====== CONTROLLI IN TESTA ======
    # Granularità: Giorno / Mese / Anno
    c1, c2, c3 = st.columns([2, 3, 3], gap="large")
    with c1:
        gran = st.radio("Resoconto:", ["Giorno", "Mese", "Anno"], horizontal=True, index=0)

    # Valutiamo i selettori in base alla granularità
    if gran == "Giorno":
        with c2:
            selected_date = st.date_input(
                "Giorno (UTC)",
                value=pd.to_datetime(_today_utc_str()).date(),
                format="YYYY-MM-DD"
            )
        day_str = _iso_date(pd.to_datetime(selected_date).to_pydatetime())

        # Toggle grafici intragiornalieri
        with c3:
            show_hr   = st.checkbox("HR (intragiornaliero)", True)
            show_spo2 = st.checkbox("SpO₂ (intragiornaliero)", True)

        st.markdown("---")

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

    elif gran == "Mese":
        # Selettore mese: usiamo una data qualsiasi del mese scelto
        with c2:
            sel_month = st.date_input(
                "Mese (scegli una data del mese)",
                value=pd.to_datetime(_today_utc_str()).date(),
                format="YYYY-MM-DD"
            )
        start_m, end_m = month_bounds(pd.to_datetime(sel_month).to_pydatetime())

        st.markdown("---")
        st.markdown(f"#### Resoconto Mensile — {start_m[:7]}")

        df = load_metrics_daily_range(str(user_id), start_m, end_m)
        if df.empty:
            st.info(f"Nessun dato disponibile per {start_m[:7]}.")
            st.stop()

        # KPI mensili
        steps = int(df["steps_total"].sum()) if "steps_total" in df else 0
        kcal  = float(df["calories_total"].sum()) if "calories_total" in df else 0.0
        hr    = float(df["hr_bpm_avg"].mean()) if "hr_bpm_avg" in df else np.nan
        spo2  = float(df["spo2_avg"].mean()) if "spo2_avg" in df else np.nan

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("HR medio (mese)", f"{hr:.0f} bpm" if not np.isnan(hr) else "—")
            st.markdown("</div>", unsafe_allow_html=True)
        with k2:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("SpO₂ media (mese)", f"{spo2:.1f} %" if not np.isnan(spo2) else "—")
            st.markdown("</div>", unsafe_allow_html=True)
        with k3:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("Passi (totale mese)", f"{steps:,}".replace(",", "."))
            st.markdown("</div>", unsafe_allow_html=True)
        with k4:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("Calorie (totale mese)", f"{kcal:.1f}")
            st.markdown("</div>", unsafe_allow_html=True)

        st.divider()

        # Grafici giorno-per-giorno nel mese
        gcols = st.columns(2)
        with gcols[0]:
            if "steps_total" in df.columns:
                st.markdown("**Passi per giorno**")
                st.bar_chart(df.set_index("event_date")[["steps_total"]], use_container_width=True)
        with gcols[1]:
            if "calories_total" in df.columns:
                st.markdown("**Calorie per giorno**")
                st.bar_chart(df.set_index("event_date")[["calories_total"]], use_container_width=True)

        gcols2 = st.columns(2)
        with gcols2[0]:
            if "hr_bpm_avg" in df.columns:
                st.markdown("**HR medio per giorno**")
                st.line_chart(df.set_index("event_date")[["hr_bpm_avg"]], use_container_width=True)
        with gcols2[1]:
            if "spo2_avg" in df.columns:
                st.markdown("**SpO₂ media per giorno**")
                st.line_chart(df.set_index("event_date")[["spo2_avg"]], use_container_width=True)

    else:  # gran == "Anno"
        with c2:
            default_year = datetime.utcnow().year
            year = st.number_input("Anno", min_value=2000, max_value=2100, value=default_year, step=1)
        start_y, end_y = year_bounds(int(year))

        st.markdown("---")
        st.markdown(f"#### Resoconto Annuale — {year}")

        df = load_metrics_daily_range(str(user_id), start_y, end_y, limit=100000)
        if df.empty:
            st.info(f"Nessun dato disponibile per l'anno {year}.")
            st.stop()

        # Aggregazione per mese
        df["month"] = pd.to_datetime(df["event_date"], utc=True).dt.to_period("M").dt.to_timestamp()

        monthly = df.groupby("month").agg({
            "steps_total": "sum",
            "calories_total": "sum",
            "hr_bpm_avg": "mean",
            "spo2_avg": "mean"
        }).reset_index()

        # KPI annuali
        steps = int(monthly["steps_total"].sum()) if "steps_total" in monthly else 0
        kcal  = float(monthly["calories_total"].sum()) if "calories_total" in monthly else 0.0
        hr    = float(monthly["hr_bpm_avg"].mean()) if "hr_bpm_avg" in monthly else np.nan
        spo2  = float(monthly["spo2_avg"].mean()) if "spo2_avg" in monthly else np.nan

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("HR medio (anno)", f"{hr:.0f} bpm" if not np.isnan(hr) else "—")
            st.markdown("</div>", unsafe_allow_html=True)
        with k2:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("SpO₂ media (anno)", f"{spo2:.1f} %" if not np.isnan(spo2) else "—")
            st.markdown("</div>", unsafe_allow_html=True)
        with k3:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("Passi (totale anno)", f"{steps:,}".replace(",", "."))
            st.markdown("</div>", unsafe_allow_html=True)
        with k4:
            st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
            st.metric("Calorie (totale anno)", f"{kcal:.1f}")
            st.markdown("</div>", unsafe_allow_html=True)

        st.divider()

        # Grafici per mese
        gcols = st.columns(2)
        with gcols[0]:
            st.markdown("**Passi per mese**")
            st.bar_chart(monthly.set_index("month")[["steps_total"]], use_container_width=True)
        with gcols[1]:
            st.markdown("**Calorie per mese**")
            st.bar_chart(monthly.set_index("month")[["calories_total"]], use_container_width=True)

        gcols2 = st.columns(2)
        with gcols2[0]:
            st.markdown("**HR medio per mese**")
            st.line_chart(monthly.set_index("month")[["hr_bpm_avg"]], use_container_width=True)
        with gcols2[1]:
            st.markdown("**SpO₂ media per mese**")
            st.line_chart(monthly.set_index("month")[["spo2_avg"]], use_container_width=True)
