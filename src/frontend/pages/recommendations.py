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

if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    st.warning("Effettua il login per accedere.")
    st.stop()

# =========================
# Authgate
# =========================
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

    # --------------------------
    # 🏋️‍♂️ Allenamento
    # --------------------------
    with col1:
        st.markdown("#### 🏋️‍♂️ Il tuo Allenamento Consigliato")

        if 'chosen_workout' not in st.session_state:
            try:
                available_workouts = db.get_available_recommendations(user_id)
            except Exception:
                st.info("⏳ Nessun consiglio di allenamento disponibile al momento.")
                available_workouts = []

            if available_workouts:
                # Usa il punteggio del ranking come peso (già esposto come success_prob)
                weights = [float(rec.get('success_prob', 1.0)) for rec in available_workouts]
                st.session_state.chosen_workout = random.choices(available_workouts, weights=weights, k=1)[0]
            else:
                st.session_state.chosen_workout = None

        if st.session_state.chosen_workout:
            chosen_workout = st.session_state.chosen_workout

            def handle_workout_like():
                try:
                    db.log_positive_feedback(user_id, chosen_workout['recommendation_id'])
                    st.toast("👍 Salvato nei preferiti!")
                except Exception as e:
                    st.error(f"Errore nel salvataggio del like: {e}")
                    return False
                finally:
                    if 'chosen_workout' in st.session_state:
                        del st.session_state.chosen_workout
                return True

            def handle_workout_dislike():
                try:
                    # niente blacklist: settiamo is_positive=0
                    db.log_negative_feedback(user_id, chosen_workout['recommendation_id'])
                    st.toast("👎 Nasconderemo questo consiglio.")
                except Exception as e:
                    st.error(f"Errore nel salvataggio del dislike: {e}")
                    return False
                finally:
                    if 'chosen_workout' in st.session_state:
                        del st.session_state.chosen_workout
                return True

            ui.render_recommendation_card(
                chosen_workout,
                key_prefix="workout",
                on_like=handle_workout_like,
                on_dislike=handle_workout_dislike
            )
        else:
            st.info("Hai già visto tutti i consigli di allenamento disponibili!")
            if st.button("Resetta consigli Allenamento", key="reset_workout", use_container_width=True):
                try:
                    if db.reset_user_workout_preferences(user_id):
                        st.toast("Preferenze allenamento resettate (tutto di nuovo visibile).")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Impossibile resettare le preferenze.")
                except Exception as e:
                    st.error(f"Errore reset: {e}")

    # --------------------------
    # 🥗 Nutrizione
    # --------------------------
    with col2:
        st.markdown("#### 🥗 Il tuo Piano Nutrizionale Consigliato")

        if 'chosen_nutrition' not in st.session_state:
            try:
                available_nutrition = db.get_available_nutrition_recommendations(user_id)
            except Exception:
                st.info("⏳ Nessun consiglio nutrizionale disponibile al momento.")
                available_nutrition = []

            if available_nutrition:
                weights = [float(rec.get('success_prob', 1.0)) for rec in available_nutrition]
                st.session_state.chosen_nutrition = random.choices(available_nutrition, weights=weights, k=1)[0]
            else:
                st.session_state.chosen_nutrition = None

        if st.session_state.chosen_nutrition:
            chosen_nutrition = st.session_state.chosen_nutrition

            def handle_nutrition_like():
                try:
                    db.log_positive_nutrition_feedback(user_id, chosen_nutrition['recommendation_id'])
                    st.toast("👍 Salvato nei preferiti!")
                except Exception as e:
                    st.error(f"Errore nel salvataggio del like: {e}")
                    return False
                finally:
                    if 'chosen_nutrition' in st.session_state:
                        del st.session_state.chosen_nutrition
                return True

            def handle_nutrition_dislike():
                try:
                    db.log_negative_nutrition_feedback(user_id, chosen_nutrition['recommendation_id'])
                    st.toast("👎 Nasconderemo questo consiglio.")
                except Exception as e:
                    st.error(f"Errore nel salvataggio del dislike: {e}")
                    return False
                finally:
                    if 'chosen_nutrition' in st.session_state:
                        del st.session_state.chosen_nutrition
                return True

            ui.render_recommendation_card(
                chosen_nutrition,
                key_prefix="nutrition",
                on_like=handle_nutrition_like,
                on_dislike=handle_nutrition_dislike
            )
        else:
            st.info("Hai già visto tutti i consigli nutrizionali disponibili!")
            if st.button("Resetta consigli Nutrizione", key="reset_nutrition", use_container_width=True):
                try:
                    if db.reset_user_nutrition_preferences(user_id):
                        st.toast("Preferenze nutrizionali resettate (tutto di nuovo visibile).")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Impossibile resettare le preferenze.")
                except Exception as e:
                    st.error(f"Errore reset: {e}")
