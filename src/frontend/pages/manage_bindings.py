# =======================
# Sensori
# =======================
with st.container(border=True):
    st.subheader("🧩 Sensori")
    sensors = db.list_user_bound_sensors_full(user_id)

    if not sensors:
        st.info("Non hai ancora sensori associati.")
    else:
        # supponiamo che la query ritorni anche "is_active"
        for (sensor_id, sensor_type_id, sensor_type_name,
             device_id, device_name, custom_name, created_at,
             is_active) in sensors:   # <-- aggiunto is_active
            with st.container(border=True):
                cols = st.columns([2, 2, 2, 1, 1])
                pretty_name = custom_name or sensor_type_name

                cols[0].markdown(f"**Sensore**: {pretty_name}")
                cols[1].markdown(f"**Tipo**: {sensor_type_name}")
                cols[2].markdown(f"**Device**: {device_name}")
                cols[3].markdown(f"**Associato**: {created_at}")

                # Stato ON/OFF con tick
                status_icon = "✅" if is_active else "❌"
                if st.button(status_icon, key=f"toggle_{sensor_id}"):
                    ok, msg, new_state = db.toggle_sensor_status(sensor_id, user_id)
                    st.success(msg) if ok else st.error(msg)
                    time.sleep(0.4);
                    st.rerun()

                # Rinomina
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
