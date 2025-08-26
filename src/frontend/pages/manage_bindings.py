# pages/manage_bindings.py
import time
import streamlit as st
from utility import database_connection as db
from frontend_utility import ui

# =========================
# Config pagina + stile
# =========================
st.set_page_config(
    page_title="Gestione dispositivi e sensori",
    layout="wide",
    initial_sidebar_state="collapsed"
)
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
    st.caption("Vedi, aggiungi, rinomina o elimina i tuoi dispositivi e sensori.")
    st.divider()

    # =======================
    # Aggiungi: Dispositivo o Sensore (senza ID in UI)
    # =======================
    with st.container(border=True):
        st.subheader("➕ Aggiungi nuovi binding")

        tabs = st.tabs(["Aggiungi dispositivo", "Aggiungi singolo sensore"])

        # -----------------------
        # TAB 1: Aggiungi dispositivo
        # -----------------------
        with tabs[0]:
            st.markdown("Associa un **dispositivo** (e i suoi sensori predefiniti).")

            # Lista device type dal catalogo
            try:
                device_types = db.get_device_types()  # [(device_type_id, name)]
            except Exception as e:
                device_types = []
                st.error(f"Errore nel recupero dei device type: {e}")

            if not device_types:
                st.info("Nessun tipo di dispositivo disponibile.")
            else:
                # Opzioni come oggetti; mostriamo solo il nome
                dt_options = [{"id": dt_id, "name": name_dt} for dt_id, name_dt in device_types]
                selected_dt = st.selectbox(
                    "Scegli il tipo di dispositivo",
                    options=dt_options,
                    format_func=lambda o: o["name"],
                    index=0,
                    key="add_dev_type_select"
                )
                selected_dt_id = selected_dt["id"]
                selected_dt_name = selected_dt["name"]

                # Preview sensori predefiniti (SOLO NOMI, senza id e senza priorità)
                with st.expander("Vedi sensori predefiniti inclusi"):
                    try:
                        preview_sensors = db.get_sensors_for_device_type(selected_dt_id)
                        # preview_sensors: [(sensor_type_id, sensor_name, priority)]
                        names = [sname for _, sname, _ in (preview_sensors or [])]
                        if names:
                            for sname in names:
                                st.markdown(f"- **{sname}**")
                        else:
                            st.caption("Nessun sensore predefinito configurato per questo device type.")
                    except Exception as e:
                        st.warning(f"Impossibile mostrare la lista sensori: {e}")

                # Nome custom del device (opzionale)
                placeholder_name = f"Es. {selected_dt_name} di {name or ''}".strip()
                custom_dev_name = st.text_input(
                    "Nome dispositivo (opzionale)",
                    placeholder=placeholder_name,
                    key="custom_dev_name_input"
                )

                # Submit
                if st.button("📱 Associa dispositivo", key="btn_bind_device"):
                    ok, msg = db.bind_device(
                        user_id=user_id,
                        device_type_id=selected_dt_id,
                        device_type_name=selected_dt_name,
                        custom_device_name=custom_dev_name.strip() if custom_dev_name else None
                    )
                    if ok:
                        st.success(msg)
                        time.sleep(0.6)
                        st.rerun()
                    else:
                        st.error(msg)

        # -----------------------
        # TAB 2: Aggiungi singolo sensore
        # -----------------------
        with tabs[1]:
            st.markdown(
                "Associa un **singolo sensore**. Se non esiste un device dedicato, "
                "verrà creato un *VirtualDevice* chiamato **SingleSensor**."
            )

            # Elenco di tutti i sensor type
            try:
                all_sensors = db.list_all_sensor_types()  # [(sensor_type_id, name)]
            except Exception as e:
                all_sensors = []
                st.error(f"Errore nel recupero dei tipi di sensore: {e}")

            if not all_sensors:
                st.info("Nessun tipo di sensore disponibile.")
            else:
                sensor_options = [{"id": sid, "name": sname} for sid, sname in all_sensors]
                selected_sensor = st.selectbox(
                    "Scegli il tipo di sensore",
                    options=sensor_options,
                    format_func=lambda o: o["name"],
                    index=0,
                    key="add_sensor_type_select"
                )
                sel_sensor_id = selected_sensor["id"]
                sel_sensor_name = selected_sensor["name"]

                custom_sensor_name = st.text_input(
                    "Nome sensore (opzionale)",
                    value=sel_sensor_name,
                    key="custom_sensor_name_input",
                    placeholder="Es. Cardio polso dx"
                )

                if st.button("🧩 Associa sensore", key="btn_bind_single_sensor"):
                    ok, msg = db.bind_single_sensor(
                        user_id=user_id,
                        sensor_type_id=sel_sensor_id,
                        custom_name=custom_sensor_name.strip() if custom_sensor_name else None
                    )
                    if ok:
                        st.success(msg)
                        time.sleep(0.6)
                        st.rerun()
                    else:
                        st.error(msg)

    st.divider()

    # =======================
    # Dispositivi (lista + azioni)
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
                                time.sleep(0.4)
                                st.rerun()
                            else:
                                st.warning("Inserisci un nuovo nome valido.")
                    with act2:
                        if st.button("🗑️ Elimina", type="secondary", key=f"btn_dev_del_{device_id}"):
                            ok, msg = db.delete_device(device_id, user_id)
                            st.success(msg) if ok else st.error(msg)
                            time.sleep(0.4)
                            st.rerun()

    st.divider()

    # =======================
    # Sensori (lista + azioni)
    # =======================
    with st.container(border=True):
        st.subheader("🧩 Sensori")
        sensors_raw = db.list_user_bound_sensors_full(user_id)

        if not sensors_raw:
            st.info("Non hai ancora sensori associati.")
        else:
            # Atteso:
            # 7 campi: (sensor_id, sensor_type_id, sensor_type_name, device_id, device_name, custom_name, created_at)
            # 8 campi: ... + is_active
            for row in sensors_raw:
                if len(row) == 8:
                    (sensor_id, sensor_type_id, sensor_type_name,
                     device_id, device_name, custom_name, created_at, is_active) = row
                else:
                    (sensor_id, sensor_type_id, sensor_type_name,
                     device_id, device_name, custom_name, created_at) = row[:7]
                    is_active = False  # default se il backend non fornisce ancora lo stato

                with st.container(border=True):
                    cols = st.columns([2, 2, 2, 1, 1])
                    pretty_name = (custom_name or sensor_type_name) or "Sensore"

                    cols[0].markdown(f"**Sensore**: {pretty_name}")
                    cols[1].markdown(f"**Tipo**: {sensor_type_name}")
                    cols[2].markdown(f"**Device**: {device_name}")
                    cols[3].markdown(f"**Associato**: {created_at}")

                    # Stato ON/OFF con tick (toggle)
                    status_icon = "✅" if is_active else "❌"
                    if cols[4].button(status_icon, key=f"toggle_{sensor_id}"):
                        ok, msg, new_state = db.toggle_sensor_status(sensor_id, user_id)
                        st.success(msg) if ok else st.error(msg)
                        time.sleep(0.4)
                        st.rerun()

                    # Rinomina sensore (non mostra ID, usa il nome attuale come default)
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
                                time.sleep(0.4)
                                st.rerun()
                            else:
                                st.warning("Inserisci un nome valido.")

                    # Consentiamo l'eliminazione SOLO se appartiene al virtual device "SingleSensor"
                    can_delete = (device_name == "SingleSensor")
                    with act2:
                        if st.button(
                                "🗑️ Elimina sensore",
                                type="secondary",
                                key=f"btn_sens_del_{sensor_id}",
                                disabled=not can_delete
                        ):
                            ok, msg = db.delete_user_sensor(sensor_id, user_id)
                            st.success(msg) if ok else st.error(msg)
                            time.sleep(0.4)
                            st.rerun()

                        if not can_delete:
                            st.caption("Eliminazione non permessa per sensori provenienti da un dispositivo.")

    st.divider()
    st.caption("Suggerimento: puoi aggiungere nuovi dispositivi da **Onboarding** o **Impostazioni**, e nuovi sensori dalla sezione qui sopra.")
