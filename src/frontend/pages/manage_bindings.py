# pages/manage_bindings.py
import time
import streamlit as st
from utility import database_connection as db
from frontend_utility import ui

# =========================
# Config pagina + stile
# =========================
st.set_page_config(page_title="Gestione dispositivi e sensori",
                   layout="wide",
                   initial_sidebar_state="collapsed")
ui.load_css()

# =========================
# Auth gate
# =========================
if "user_id" not in st.session_state or not st.session_state.get("logged_in", False):
    st.error("Errore: utente non autenticato.")
    st.stop()
user_id = st.session_state["user_id"]

# =========================
# Sidebar + layout (convenzione progetto)
# =========================
name, surname = db.retrive_name(user_id)

sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

with main_col:
    st.title("🔧 Gestione dispositivi e sensori")
    st.caption("Vedi, rinomina o elimina i tuoi dispositivi e sensori.")
    st.divider()

    # =======================
    # Dispositivi
    # =======================
    with st.container(border=True):
        st.subheader("📱 Dispositivi")
        devices = db.list_user_devices_detailed(user_id)

        if not devices:
            st.info("Non hai ancora dispositivi associati.")
        else:
            for (device_id, device_name, device_type_name, registered_at) in devices:
                with st.container(border=True):
                    cols = st.columns([2, 2, 2, 2])
                    cols[0].markdown(f"**Nome**: {device_name}")
                    cols[1].markdown(f"**Tipo**: {device_type_name}")
                    cols[2].markdown(f"**Registrato**: {registered_at}")

                    new_name = cols[3].text_input(
                        "Nuovo nome",
                        key=f"dev_rename_{device_id}",
                        placeholder="Inserisci nuovo nome"
                    )

                    act1, act2 = st.columns([1, 1])
                    with act1:
                        if st.button("✏️ Rinomina", key=f"btn_dev_rename_{device_id}"):
                            if new_name and new_name.strip():
                                ok, msg = db.rename_device(device_id, user_id, new_name.strip())
                                st.success(msg) if ok else st.error(msg)
                                time.sleep(0.4); st.rerun()
                            else:
                                st.warning("Inserisci un nuovo nome valido.")
                    with act2:
                        if st.button("🗑️ Elimina", type="secondary", key=f"btn_dev_del_{device_id}"):
                            ok, msg = db.delete_device(device_id, user_id)
                            st.success(msg) if ok else st.error(msg)
                            time.sleep(0.4); st.rerun()

    st.divider()

    # =======================
    # Sensori
    # =======================
    with st.container(border=True):
        st.subheader("🧩 Sensori")
        sensors = db.list_user_bound_sensors_full(user_id)

        if not sensors:
            st.info("Non hai ancora sensori associati.")
        else:
            for (sensor_id, sensor_type_id, sensor_type_name, device_id, device_name, custom_name, created_at) in sensors:
                with st.container(border=True):
                    cols = st.columns([2, 2, 2, 2])
                    # Nome visuale: custom se presente, altrimenti il tipo
                    pretty_name = custom_name or sensor_type_name
                    cols[0].markdown(f"**Sensore**: {pretty_name}")
                    cols[1].markdown(f"**Tipo**: {sensor_type_name}")
                    cols[2].markdown(f"**Device**: {device_name}")
                    cols[3].markdown(f"**Associato**: {created_at}")

                    new_s_name = st.text_input(
                        "Nuovo nome sensore",
                        key=f"sens_rename_{sensor_id}",
                        placeholder="es. Cardio polso dx",
                        value=pretty_name
                    )

                    act1, act2 = st.columns([1, 1])
                    with act1:
                        if st.button("✏️ Rinomina sensore", key=f"btn_sens_rename_{sensor_id}"):
                            if new_s_name and new_s_name.strip():
                                ok, msg = db.rename_user_sensor(sensor_id, user_id, new_s_name.strip())
                                st.success(msg) if ok else st.error(msg)
                                time.sleep(0.4); st.rerun()
                            else:
                                st.warning("Inserisci un nome valido.")
                    with act2:
                        if st.button("🗑️ Elimina sensore", type="secondary", key=f"btn_sens_del_{sensor_id}"):
                            ok, msg = db.delete_user_sensor(sensor_id, user_id)
                            st.success(msg) if ok else st.error(msg)
                            time.sleep(0.4); st.rerun()

    st.divider()
    st.caption("Suggerimento: puoi aggiungere nuovi dispositivi da **Onboarding** o **Impostazioni**, e nuovi sensori dalla pagina **Associa sensori**.")
