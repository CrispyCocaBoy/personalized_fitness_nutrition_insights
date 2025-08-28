# pages/recommendations.py
import streamlit as st
from utility import database_connection as db
from frontend_utility import ui
import time
import random

# =========================
# Config pagina + stile
# =========================
st.set_page_config(page_title="Consigli del Giorno", layout="wide", initial_sidebar_state="collapsed")
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
# Layout
# =========================
sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

with main_col:
    ui.render_header("Consigli del Giorno", "Le nostre raccomandazioni personalizzate per te")

    col1, col2 = st.columns(2)

    # --- Sezione Allenamento con MEMORIA (Spostata qui) ---
    with col1:
        st.markdown("#### 🏋️‍♂️ Il tuo Allenamento Consigliato")

        if 'chosen_workout' not in st.session_state:
            available_workouts = db.get_available_recommendations(user_id)
            if available_workouts:
                weights = [float(rec['success_prob']) for rec in available_workouts]
                st.session_state.chosen_workout = random.choices(available_workouts, weights=weights, k=1)[0]
            else:
                st.session_state.chosen_workout = None

        if st.session_state.chosen_workout:
            chosen_workout = st.session_state.chosen_workout
            
            def handle_workout_like():
                db.log_positive_feedback(user_id, chosen_workout['recommendation_id'])
                del st.session_state.chosen_workout
                return True

            def handle_workout_dislike():
                db.add_to_blacklist(user_id, chosen_workout['recommendation_id'])
                del st.session_state.chosen_workout
                return True
            
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

    # --- Sezione Nutrizione con MEMORIA (Spostata qui) ---
    with col2:
        st.markdown("#### 🥗 Il tuo Piano Nutrizionale Consigliato")

        if 'chosen_nutrition' not in st.session_state:
            available_nutrition = db.get_available_nutrition_recommendations(user_id)
            if available_nutrition:
                weights = [float(rec['success_prob']) for rec in available_nutrition]
                st.session_state.chosen_nutrition = random.choices(available_nutrition, weights=weights, k=1)[0]
            else:
                st.session_state.chosen_nutrition = None

        if st.session_state.chosen_nutrition:
            chosen_nutrition = st.session_state.chosen_nutrition
            
            def handle_nutrition_like():
                db.log_positive_nutrition_feedback(user_id, chosen_nutrition['recommendation_id'])
                del st.session_state.chosen_nutrition
                return True

            def handle_nutrition_dislike():
                db.add_to_nutrition_blacklist(user_id, chosen_nutrition['recommendation_id'])
                del st.session_state.chosen_nutrition
                return True

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