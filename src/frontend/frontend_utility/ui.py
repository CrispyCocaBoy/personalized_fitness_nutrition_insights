# frontend_utility/ui.py
import streamlit as st
import time
from utility import database_connection as db

# questo contiene la sidebar uguale per tutte le pagine

# BASE_CSS rimane invariato...
BASE_CSS = """
<style>
.block-container{max-width:1500px;padding-top:.8rem;padding-bottom:2rem;}
.profile-card{border:1px solid rgba(0,0,0,.08);padding:12px;border-radius:12px;background:rgba(0,0,0,.02);}
.nav a{display:block;padding:6px 8px;
    border-radius:10px;
    margin:4px 0;text-decoration:none;}
.nav a:hover{background:#f3f4f6;}
.kpi-card{border:1px solid rgba(0,0,0,.08);
    padding:12px;
    border-radius:12px;
    background:#fff;}
.block-container {
    max-width: 1500px;
    padding-top: 2rem;
    padding-bottom: 2rem;
}
</style>
"""

def load_css():
    st.markdown(BASE_CSS, unsafe_allow_html=True)

def render_sidebar(name: str, surname: str, user_id: str):
    st.markdown("## 👤 ")
    st.markdown(
        f"""
        <div class="profile-card">
            <div style="font-weight:600; font-size:1.05rem;">{name} {surname}</div>
            <div style="opacity:0.7; font-size:0.9rem;">ID: {user_id}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.markdown("## 🧭 ")
    # usa sempre questi link: saranno uguali ovunque
    st.page_link("pages/dashboard.py", label="Home", icon="🏠")
    st.page_link("pages/health.py", label="Heart", icon="❤️")
    st.page_link("pages/recommendations.py", label="Consigli", icon="💡")
    st.page_link("pages/meals.py", label="Meals", icon="🍽️")
    st.page_link("pages/activity.py", label="Activity", icon="🔥")
    st.page_link("pages/settings.py", label="Setting", icon="⚙️")
    st.page_link("pages/sleep.py", label="Sleep", icon="🛌")

# render_header e render_recommendation_card rimangono invariati...
def render_header(title: str, subtitle: str, settings_page="pages/settings.py"):
    left, right = st.columns([6, 1])
    with left:
        st.title(title)
        st.caption(subtitle)

def render_recommendation_card(recommendation: dict, key_prefix: str, on_like, on_dislike):
    rec_id = recommendation['recommendation_id']
    # Modifica per generalizzare il titolo
    title = recommendation.get('workout_type') or recommendation.get('nutrition_plan') or "Consiglio"
    details = recommendation['details']

    with st.container(border=True):
        st.subheader(title)
        st.write(details)
        
        b_col1, b_col2 = st.columns(2)
        with b_col1:
            if st.button("Fatto 👍", key=f"{key_prefix}_like_{rec_id}", use_container_width=True):
                if on_like():
                    st.toast("Grazie per il tuo feedback!")
                else:
                    st.toast("Errore nel salvataggio del feedback.")
                time.sleep(1)
                st.rerun()

        with b_col2:
            if st.button("Non mi piace 👎", key=f"{key_prefix}_dislike_{rec_id}", use_container_width=True):
                if on_dislike():
                    st.toast("Ok, non te lo mostreremo più.")
                else:
                    st.toast("Errore nell'aggiornamento.")
                time.sleep(1)
                st.rerun()
