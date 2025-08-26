import streamlit as st
from utility import database_connection as db
from frontend_utility import ui
import time
import random # <-- Assicurati che questo import sia presente

st.set_page_config(page_title="Dashboard", layout="wide", initial_sidebar_state="collapsed")
ui.load_css()

if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    st.warning("Effettua il login per accedere.")
    st.stop()

user_id = st.session_state["user_id"]
name, surname = db.retrive_name(user_id)

sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

# --- INIZIA LA NUOVA LOGICA ---

with main_col:
    ui.render_header(f"Benvenuto, {name}!", "La tua dashboard personale")

    col1, col2 = st.columns(2)

    # --- Sezione Allenamento con MEMORIA ---
    with col1:
        st.markdown("#### 🏋️‍♂️ Il tuo Allenamento Consigliato")

        # 1. Se non abbiamo un allenamento in memoria, ne scegliamo uno nuovo
        if 'chosen_workout' not in st.session_state:
            available_workouts = db.get_available_recommendations(user_id)
            if available_workouts:
                weights = [float(rec['success_prob']) for rec in available_workouts]
                st.session_state.chosen_workout = random.choices(available_workouts, weights=weights, k=1)[0]
            else:
                st.session_state.chosen_workout = None # Nessun allenamento disponibile

        # 2. Mostriamo l'allenamento (se esiste) o il messaggio di reset
        if st.session_state.chosen_workout:
            chosen_workout = st.session_state.chosen_workout
            
            # 3. I pulsanti ora devono cancellare la memoria per forzare una nuova scelta
            def handle_workout_like():
                db.log_positive_feedback(user_id, chosen_workout['recommendation_id'])
                del st.session_state.chosen_workout # Dimentica questo allenamento

            def handle_workout_dislike():
                db.add_to_blacklist(user_id, chosen_workout['recommendation_id'])
                del st.session_state.chosen_workout # Dimentica questo allenamento
            
            ui.render_recommendation_card(
                chosen_workout,
                key_prefix="workout",
                on_like=handle_workout_like,
                on_dislike=handle_workout_dislike
            )
        else:
            st.info("Hai già visto tutti i consigli di allenamento per oggi!")
            if st.button("Resetta consigli Allenamento", key="reset_workout", use_container_width=True):
                if db.reset_blacklist(user_id):
                    st.toast("Fatto! Ricarica per vedere i nuovi consigli.")
                    time.sleep(1)
                    st.rerun()

    # --- Sezione Nutrizione con MEMORIA ---
    with col2:
        st.markdown("#### 🥗 Il tuo Piano Nutrizionale Consigliato")

        # 1. Logica identica per la nutrizione
        if 'chosen_nutrition' not in st.session_state:
            available_nutrition = db.get_available_nutrition_recommendations(user_id)
            if available_nutrition:
                weights = [float(rec['success_prob']) for rec in available_nutrition]
                st.session_state.chosen_nutrition = random.choices(available_nutrition, weights=weights, k=1)[0]
            else:
                st.session_state.chosen_nutrition = None

        # 2. Mostriamo la nutrizione (se esiste) o il messaggio di reset
        if st.session_state.chosen_nutrition:
            chosen_nutrition = st.session_state.chosen_nutrition
            
            # 3. Pulsanti specifici per la nutrizione
            def handle_nutrition_like():
                db.log_positive_nutrition_feedback(user_id, chosen_nutrition['recommendation_id'])
                del st.session_state.chosen_nutrition

            def handle_nutrition_dislike():
                db.add_to_nutrition_blacklist(user_id, chosen_nutrition['recommendation_id'])
                del st.session_state.chosen_nutrition

            ui.render_recommendation_card(
                chosen_nutrition,
                key_prefix="nutrition",
                on_like=handle_nutrition_like,
                on_dislike=handle_nutrition_dislike
            )
        else:
            st.info("Hai già visto tutti i consigli nutrizionali per oggi!")
            if st.button("Resetta consigli Nutrizione", key="reset_nutrition", use_container_width=True):
                if db.reset_nutrition_blacklist(user_id):
                    st.toast("Fatto! Ricarica per vedere i nuovi consigli.")
                    time.sleep(1)
                    st.rerun()

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