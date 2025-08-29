# utility/ui.py
import streamlit as st

# questo contiene la sidebar uguale per tutte le pagine

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
.recommendation-card {
    background-color: rgba(255, 255, 255, 0.05);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 1rem;
    min-height: 180px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
}
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
    st.page_link("pages/meals.py", label="Pasti", icon="🍽️")
    st.page_link("pages/activity.py", label="Attività", icon="🔥")
    st.page_link("pages/recommendations.py", label="Consigli", icon="💡")
    st.page_link("pages/settings.py", label="Impostazioni", icon="⚙️")


def render_header(title: str, subtitle: str, settings_page="pages/settings.py"):
    left, right = st.columns([6, 1])
    with left:
        st.title(title)
        st.caption(subtitle)


def render_recommendation_card(recommendation, key_prefix, on_like, on_dislike):
    """
    Renderizza una card per un singolo consiglio con i pulsanti di feedback.
    """
    rec_id = recommendation['recommendation_id']

    with st.container():
        st.markdown(
            f'<div class="recommendation-card">{recommendation["description"]}</div>',
            unsafe_allow_html=True
        )

        b_col1, b_col2 = st.columns(2)
        with b_col1:
            if st.button("👍 Mi piace", key=f"{key_prefix}_like_{rec_id}", use_container_width=True):
                if on_like():
                    st.toast("Grazie per il tuo feedback!")
                    st.rerun()

        with b_col2:
            if st.button("👎 Non mi piace", key=f"{key_prefix}_dislike_{rec_id}", use_container_width=True):
                if on_dislike():
                    st.toast("Consiglio rimosso. Grazie!")
                    st.rerun()