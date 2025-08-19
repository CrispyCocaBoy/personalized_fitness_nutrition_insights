# utility/ui.py
import streamlit as st

BASE_CSS = """
<style>
.block-container{max-width:1500px;padding-top:.8rem;padding-bottom:2rem;}
.profile-card{border:1px solid rgba(0,0,0,.08);padding:12px;border-radius:12px;background:rgba(0,0,0,.02);}
.nav a{display:block;padding:6px 8px;border-radius:10px;margin:4px 0;text-decoration:none;}
.nav a:hover{background:#f3f4f6;}
.kpi-card{border:1px solid rgba(0,0,0,.08);padding:12px;border-radius:12px;background:#fff;}
</style>
"""

def load_css():
    st.markdown(BASE_CSS, unsafe_allow_html=True)

def render_sidebar(name: str, surname: str, user_id: str):
    st.markdown("### 👤 Profilo")
    st.markdown(
        f"""
        <div class="profile-card">
            <div style="font-weight:600; font-size:1.05rem;">{name} {surname}</div>
            <div style="opacity:0.7; font-size:0.9rem;">ID: {user_id}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.markdown("### 🧭 Navigazione")
    # usa sempre questi link: saranno uguali ovunque
    st.page_link("pages/dashboard.py", label="Home", icon="🏠")
    st.page_link("pages/meals.py", label="Pasti", icon="🍽️")
    st.page_link("pages/settings.py", label="Impostazioni", icon="⚙️")

def render_header(title: str, subtitle: str, settings_page="pages/settings.py"):
    left, right = st.columns([6, 1])
    with left:
        st.title(title)
        st.caption(subtitle)
    with right:
        if st.button("⚙️", help="Impostazioni", use_container_width=True):
            st.switch_page(settings_page)
