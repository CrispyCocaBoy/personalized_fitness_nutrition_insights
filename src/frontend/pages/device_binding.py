# pages/device_binding.py
import time
import streamlit as st
from utility import database_connection as db

# =========================
# Config pagina + stile
# =========================
st.set_page_config(
    page_title="Collega il tuo dispositivo",
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

# Mostra la pagina solo durante la registrazione
if not st.session_state.get("registration_flow", True):
    st.info("Questa pagina è disponibile solo durante la registrazione.")
    st.stop()

# =========================
# Helpers DB
# =========================
def _get_user_name(uid: int) -> str:
    try:
        name, _surname = db.retrive_name(uid)
        return name
    except Exception:
        return "User"

def _sensor_names_for_device_type(device_type_id: int) -> list[str]:
    rows = db.get_sensors_for_device_type(device_type_id)
    return [row[1] for row in rows] if rows else []

# =========================
# UI
# =========================
st.title("🔌 Collega il tuo dispositivo")
st.caption("Passo 2 di 2 — Collega il tuo wearable o dispositivo per iniziare a raccogliere i dati.")

user_name = _get_user_name(user_id)
st.markdown(f"👋 Ciao **{user_name}**! Scegli il tipo di dispositivo che vuoi associare al tuo account.")

# CTA alternativa
alt_col1, alt_col2 = st.columns([1, 2])
with alt_col1:
    if st.button("👉 Preferisco associare un sensore singolo"):
        st.switch_page("pages/sensor_binding.py")
with alt_col2:
    st.caption("Puoi sempre aggiungerne altri in seguito dalle **Impostazioni → Dispositivi**.")

st.divider()

# Carica device types
device_types = db.get_device_types()
if not device_types:
    st.warning("Nessun tipo di dispositivo predefinito trovato. Contatta il supporto.")
    st.stop()

# Mapping per selectbox
type_labels = [name for (_id, name) in device_types]
type_id_by_label = {name: _id for (_id, name) in device_types}

# Selettore live
selected_label = st.selectbox(
    "Tipo di dispositivo",
    options=type_labels,
    index=0,
    key="device_type_select"
)
selected_type_id = type_id_by_label[selected_label]

# Nome dispositivo (opzionale)
custom_device_name = st.text_input(
    "Nome dispositivo (opzionale)",
    placeholder=f"{selected_label} of {user_name}"
).strip()

# Card riassuntiva
with st.container(border=True):
    st.subheader("📦 Riepilogo associazione")
    c1, c2 = st.columns([1, 2], vertical_alignment="top")

    with c1:
        st.markdown("**Dispositivo scelto**")
        st.write(selected_label)

        st.markdown("**Nome che verrà salvato**")
        preview_name = custom_device_name or f"{selected_label} of {user_name}"
        st.write(preview_name)

        # Azione: associa dispositivo
        if st.button("✅ Associa dispositivo", use_container_width=True):
            ok, msg = db.bind_device(
                user_id=user_id,
                device_type_id=selected_type_id,
                device_type_name=selected_label,
                custom_device_name=custom_device_name if custom_device_name else None
            )
            if ok:
                st.success(msg)
                st.session_state["registration_flow"] = False
                st.session_state["onboarding_done"] = True
                time.sleep(0.8)
                st.switch_page("pages/dashboard.py")
            else:
                st.error(msg)

        # Azione: salta
        if st.button("⏭️ Salta per ora", use_container_width=True):
            st.info("Hai saltato l’associazione del dispositivo. Potrai farlo in seguito dalle impostazioni.")
            st.session_state["registration_flow"] = False
            st.session_state["onboarding_done"] = True
            time.sleep(0.4)
            st.switch_page("pages/dashboard.py")

    with c2:
        st.markdown("**Sensori che verranno associati automaticamente**")
        sensor_names = _sensor_names_for_device_type(selected_type_id)
        if sensor_names:
            st.markdown("\n".join([f"• {n}" for n in sensor_names]))
        else:
            st.caption("Nessun sensore predefinito per questo tipo di dispositivo.")

st.divider()
st.caption("Suggerimento: puoi aggiungere o rimuovere dispositivi e sensori in qualsiasi momento dalle **Impostazioni**.")
