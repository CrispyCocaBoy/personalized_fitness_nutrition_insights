# pages/sensor_binding.py
import time
import streamlit as st
from utility import database_connection as db

# =========================
# Config pagina + stile
# =========================
st.set_page_config(
    page_title="Associa sensori singoli",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# =========================
# Auth / Onboarding gate
# =========================
if "user_id" not in st.session_state or not st.session_state.get("logged_in", False):
    st.error("Errore: utente non autenticato.")
    st.stop()

user_id = st.session_state["user_id"]

# Mostrala solo in onboarding
if not st.session_state.get("registration_flow", True):
    st.info("Questa pagina è disponibile solo durante la registrazione.")
    st.stop()

# =========================
# Helpers
# =========================
def _get_user_name(uid: int) -> str:
    try:
        name, _surname = db.retrive_name(uid)
        return name
    except Exception:
        return "User"

# =========================
# UI
# =========================
st.title("🧩 Associa sensori singoli")
st.caption("Alternativa all’associazione di un dispositivo — collega uno o più sensori direttamente al tuo account.")

user_name = _get_user_name(user_id)
st.write(f"Ciao **{user_name}**! Se preferisci, puoi associare direttamente i singoli sensori senza passare da un dispositivo.")

# =========================
# Selezione sensore
# =========================
sensor_types = db.list_all_sensor_types()
if not sensor_types:
    st.error("Nessun tipo di sensore disponibile. Contatta il supporto.")
    st.stop()

sensor_labels = [name for (_sid, name) in sensor_types]
sensor_id_by_label = {name: _sid for (_sid, name) in sensor_types}

with st.form("bind_single_sensor_form", clear_on_submit=False):
    st.subheader("Collega un nuovo sensore")

    selected_sensor_label = st.selectbox("Tipo di sensore", options=sensor_labels, index=0)
    selected_sensor_id = sensor_id_by_label[selected_sensor_label]

    custom_name = st.text_input("Nome personalizzato (opzionale)", placeholder="es. Cardio polso dx")

    submitted = st.form_submit_button("✅ Associa sensore")

    if submitted:
        ok, msg = db.bind_single_sensor(
            user_id=user_id,
            sensor_type_id=selected_sensor_id,
            custom_name=custom_name.strip() if custom_name else None
        )
        if ok:
            st.success(msg)
            st.session_state["registration_flow"] = False
            st.session_state["onboarding_done"] = True
            time.sleep(0.6)
            st.switch_page("pages/dashboard.py")
        else:
            st.error(msg)

st.divider()

# =========================
# Azioni di flusso
# =========================
cols = st.columns([1, 1, 1])
with cols[0]:
    if st.button("🔌 Vai all’associazione dispositivo"):
        st.switch_page("pages/device_binding.py")

with cols[1]:
    if st.button("⏭️ Salta per ora"):
        st.info("Hai saltato l’associazione sensori. Potrai farlo in seguito dalle impostazioni.")
        st.session_state["registration_flow"] = False
        st.session_state["onboarding_done"] = True
        time.sleep(0.4)
        st.switch_page("pages/dashboard.py")

with cols[2]:
    st.caption("Potrai aggiungere sensori in seguito da **Impostazioni → Dispositivi/Sensori**.")
