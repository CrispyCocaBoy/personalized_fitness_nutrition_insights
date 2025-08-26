# pages/meals.py
import os
import uuid
from datetime import datetime, date, time as dtime, timezone
from zoneinfo import ZoneInfo
import time
import streamlit as st

from utility import database_connection as db
from frontend_utility import ui  # sidebar + header + css comuni

# =========================
# Config pagina + stile
# =========================
st.set_page_config(page_title="Pasti", layout="wide", initial_sidebar_state="collapsed")
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
# Backend adapter (HTTP o DB)
# =========================
BACKEND_URL = "http://gateway:8000"


def _backend() -> str | None:
    if not BACKEND_URL:
        return None
    return BACKEND_URL.rstrip("/")


def _iso_utc_now() -> str:
    # ISO8601 con suffisso Z (UTC)
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _to_iso_utc_from_local(day: date, at: dtime, tz: str = "Europe/Rome") -> str:
    """Converte (giorno, ora) locali in ISO8601 UTC 'Z'."""
    naive = datetime.combine(day, at).replace(microsecond=0)
    local_dt = naive.replace(tzinfo=ZoneInfo(tz))
    utc_dt = local_dt.astimezone(timezone.utc)
    return utc_dt.isoformat().replace("+00:00", "Z")


def send_meal(payload: dict) -> tuple[bool, str]:
    """
    Invia il pasto al gateway (ingestion) via POST /api/meals.
    Il gateway lo instrada su Kafka; lo streaming job scrive nelle tabelle gold_meals_*.
    """
    be = _backend()
    try:
        import requests
        url = f"{be}/api/meals"  # endpoint di ingest
        # Normalizza timestamp in UTC Z se manca
        if "timestamp" not in payload or not payload["timestamp"]:
            payload["timestamp"] = _iso_utc_now()
        payload["user_id"] = str(payload["user_id"])
        r = requests.post(url, json=payload, timeout=6)
        if r.ok:
            ack = r.json()
            mid = ack.get("meal_id") or payload.get("meal_id")
            return True, f"Pasto inviato al backend. id={mid}"
        return False, f"Errore backend: {r.status_code} {r.text}"
    except Exception as e:
        return False, f"Errore di rete: {e}"


def delete_meal(meal_id: str, user_id: str, timestamp: str) -> tuple[bool, str]:
    """
    Elimina un pasto specifico via DELETE /api/meals/{meal_id}.
    Il gateway richiede anche il timestamp originale per ricalcolare la daily.
    """
    be = _backend()
    if not be:
        return False, "Backend non configurato"
    try:
        import requests
        url = f"{be}/api/meals/{meal_id}"
        params = {"user_id": str(user_id), "timestamp": timestamp}
        r = requests.delete(url, params=params, timeout=10)
        if r.ok:
            return True, "Pasto eliminato "
        else:
            error_detail = r.json().get("detail", r.text) if r.headers.get("content-type", "").startswith(
                "application/json") else r.text
            return False, f"Errore backend: {r.status_code} - {error_detail}"
    except Exception as e:
        return False, f"Errore di rete: {e}"


def load_meals(user_id: str, limit: int = 25, start_date: str | None = None, end_date: str | None = None) -> list[dict]:
    """
    Recupera gli ultimi pasti dai FACT via API /meals/facts (meal_facts).
    Accetta opzionalmente un range data (YYYY-MM-DD).
    """
    be = _backend()
    if not be:
        return []
    try:
        import requests
        params = {"user_id": str(user_id), "limit": int(limit)}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        url = f"{be}/meals/facts"
        r = requests.get(url, params=params, timeout=20)
        if r.ok:
            return r.json()
        else:
            st.warning(f"Errore backend: {r.status_code} {r.text}")
            return []
    except Exception as e:
        st.warning(f"Errore di rete: {e}")
        return []


def load_meals_daily(user_id: str, start_date: str | None = None, end_date: str | None = None, limit: int = 30) -> list[
    dict]:
    """
    Recupera aggregati giornalieri via API /meals/daily (meal_daily).
    """
    be = _backend()
    if not be:
        return []
    try:
        import requests
        params = {"user_id": str(user_id), "limit": int(limit)}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        url = f"{be}/meals/daily"
        r = requests.get(url, params=params, timeout=20)
        return r.json() if r.ok else []
    except Exception:
        return []


# =========================
# Helpers UI
# =========================
def _normalize_items(items):
    norm = []
    for it in (items or []):
        if isinstance(it, str):
            norm.append({
                "user_food_id": None,
                "name": it, "kcal": 0, "carbs_g": 0, "protein_g": 0, "fat_g": 0,
                "quantity": None, "unit": None,
                "source": "default"
            })
        elif isinstance(it, dict):
            # infer source se presente la chiave user_food_id
            source = "personalized" if (it.get("user_food_id") or it.get("id") or it.get("food_id")) else "default"
            norm.append({
                "user_food_id": it.get("user_food_id") or it.get("id") or it.get("food_id"),
                "name": it.get("name") or "Alimento",
                "kcal": int(float(it.get("kcal") or it.get("calories") or 0)),
                "carbs_g": int(float(it.get("carbs_g") or it.get("carbohydrates") or 0)),
                "protein_g": int(float(it.get("protein_g") or it.get("protein") or 0)),
                "fat_g": int(float(it.get("fat_g") or it.get("fat") or 0)),
                "quantity": it.get("quantity"),
                "unit": it.get("unit"),
                "category": it.get("category") or None,
                "source": source
            })
    return norm


def _grid_buttons(items, source_label: str, cols=3):
    items = _normalize_items(items)
    if not items:
        st.info("Nessun alimento disponibile.")
        return
    for i in range(0, len(items), cols):
        row = st.columns(cols)
        for col, item in zip(row, items[i:i + cols]):
            qty_label = f"{item['quantity']} {item['unit']}" if item["quantity"] and item["unit"] else ""
            title = f"{item['name']}" + (f" ({qty_label})" if qty_label else "")

            kcal = item.get("kcal", 0)
            carbs = item.get("carbs_g", 0)
            prot = item.get("protein_g", 0)
            fat = item.get("fat_g", 0)

            sub = f"🔥 {kcal} kcal · 🥖 {carbs}C / 🥚 {prot}P / 🫒 {fat}F"

            with col:
                st.markdown(
                    f'''
                    <div class="card" style="text-align:center; font-weight:500;">
                        {title}
                        <div class="small" style="margin-top:6px;">{sub}</div>
                    </div>
                    ''',
                    unsafe_allow_html=True
                )
                if st.button("➕ Aggiungi", use_container_width=True, key=f"qa_{source_label}_{i}_{title}"):
                    # 🔎 DB lookup in base alla fonte
                    if source_label in ("default", "search"):
                        food = db.get_food_by_name(item["name"])
                    elif source_label == "personalized":
                        food = db.get_personalized_food_by_name(user_id, item["name"])
                    else:
                        food = None

                    if not food:
                        st.error("Errore: alimento non trovato nel DB")
                    else:
                        payload = {
                            "meal_id": str(uuid.uuid4()),
                            "user_id": user_id,
                            "meal_name": food["name"],
                            "kcal": int(float(food.get("calories") or 0)),
                            "carbs_g": int(float(food.get("carbohydrates") or 0)),
                            "protein_g": int(float(food.get("protein") or 0)),
                            "fat_g": int(float(food.get("fat") or 0)),
                            "timestamp": _iso_utc_now(),
                            "notes": f"quick add ({source_label})",
                            "quantity": float(food.get("quantity") or 0),
                            "unit": food.get("unit") or None,
                            "category": food.get("category") or None,
                        }
                        ok, msg = send_meal(payload)
                        if ok:
                            st.success("Pasto aggiunto ✅")
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error(msg)


def _grid_personalized(items, cols=3):
    items = _normalize_items(items)
    if not items:
        st.info("Non hai ancora alimenti personalizzati.")
        return
    for i in range(0, len(items), cols):
        row = st.columns(cols)
        for col, item in zip(row, items[i:i + cols]):
            qty_label = f"{item['quantity']} {item['unit']}" if item["quantity"] and item["unit"] else ""
            title = f"{item['name']}" + (f" ({qty_label})" if qty_label else "")
            sub = f"{item['kcal']} kcal"
            macro = ""
            if any([item["carbs_g"], item["protein_g"], item["fat_g"]]):
                macro = f" · {item['carbs_g']}C/{item['protein_g']}P/{item['fat_g']}F"

            with col:
                st.markdown(
                    f'<div class="card" style="text-align:center; font-weight:500;">{title}'
                    f'<div class="small" style="margin-top:6px;">{sub}{macro}</div></div>',
                    unsafe_allow_html=True
                )
                cadd, cdel = st.columns([0.6, 0.4])
                with cadd:
                    if st.button("➕ Aggiungi",
                                 use_container_width=True,
                                 key=f"p_add_{item.get('user_food_id')}_{i}"):
                        food = db.get_personalized_food_by_name(user_id, item["name"])
                        if not food:
                            st.error("Errore: alimento personalizzato non trovato")
                        else:
                            payload = {
                                "meal_id": str(uuid.uuid4()),
                                "user_id": user_id,
                                "meal_name": food["name"],
                                "kcal": int(float(food.get("calories") or 0)),
                                "carbs_g": int(float(food.get("carbohydrates") or 0)),
                                "protein_g": int(float(food.get("protein") or 0)),
                                "fat_g": int(float(food.get("fat") or 0)),
                                "timestamp": _iso_utc_now(),
                                "notes": "quick add (personalized)",
                                "quantity": float(food.get("quantity") or 0),
                                "unit": food.get("unit") or None,
                                "category": food.get("category") or None,
                            }
                            ok, msg = send_meal(payload)
                            if ok:
                                st.success("Pasto aggiunto ✅")
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error(msg)

                with cdel:
                    disabled = item.get("user_food_id") is None
                    if st.button("🗑️ Elimina",
                                 use_container_width=True,
                                 key=f"p_del_{item.get('user_food_id')}_{i}",
                                 disabled=disabled):
                        k = f"confirm_del_{item['user_food_id']}"
                        if not st.session_state.get(k):
                            st.session_state[k] = True
                            st.warning("Clicca di nuovo per confermare l'eliminazione.")
                        else:
                            try:
                                ok = db.delete_personalized_food(
                                    user_food_id=int(item["user_food_id"]),
                                    user_id=int(user_id),
                                )
                                if ok:
                                    st.success("Alimento eliminato ✅")
                                else:
                                    st.info("Nessun elemento trovato (può essere già eliminato).")
                                time.sleep(2)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Errore eliminazione: {e}")


# =========================
# Layout a 2 colonne
# =========================
sidebar_col, main_col = st.columns([0.9, 6.1], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

with main_col:
    ui.render_header("Pasti", "Registra rapidamente i pasti e monitora calorie/macros.")

    # ======= Selettore modalità (in alto) =======
    mode = st.radio(
        "Seleziona se vuoi vedere i consumi del giorno o aggiungere un pasto",
        options=["📆 Consumi del giorno", "➕ Aggiungi pasto"],
        horizontal=True,
        index=0
    )

    # =========================
    # Modalità 1: Consumi del giorno
    # =========================
    if mode == "📆 Consumi del giorno":
        c1, c2 = st.columns([1.2, 0.3])
        with c1:
            sel_date: date = st.date_input("Giorno", value=date.today(), format="YYYY-MM-DD")
        with c2:
            st.write("")  # spacing
            refresh = st.button("🔄 Ricarica", use_container_width=True)

        start = sel_date.strftime("%Y-%m-%d")
        end = start  # stesso giorno
        meals_day = load_meals(user_id, limit=500, start_date=start, end_date=end)

        # Calcolo totali
        tot_kcal = sum(int(m.get("kcal") or 0) for m in meals_day)
        tot_c = sum(int(m.get("carbs_g") or 0) for m in meals_day)
        tot_p = sum(int(m.get("protein_g") or 0) for m in meals_day)
        tot_f = sum(int(m.get("fat_g") or 0) for m in meals_day)

        st.markdown("#### Riepilogo giornaliero")
        st.markdown(f"""
        <div class="card" style="display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
            <div style="font-weight:700;">{start}</div>
            <div class="badge">🔥 {tot_kcal} kcal</div>
            <div class="badge">🥖 {tot_c} g</div>
            <div class="badge">🥚 {tot_p} g</div>
            <div class="badge">🫒 {tot_f} g</div>
            <div class="badge">🍽️ {len(meals_day)} pasti</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("#### Dettaglio pasti del giorno")
        if not meals_day:
            st.info("Nessun pasto registrato per il giorno selezionato.")
        else:
            # Mostro i pasti con possibilità di eliminazione
            for idx, m in enumerate(meals_day):
                meal_id = m.get("meal_id")
                meal_name = m.get("meal_name") or m.get("name") or "Pasto"
                ts = m.get("event_ts") or m.get("timestamp") or m.get("ts")
                kcal = m.get("kcal", 0)
                carbs = m.get("carbs_g", 0)
                protein = m.get("protein_g", 0)
                fat = m.get("fat_g", 0)
                notes = m.get("notes", "")

                # Orario locale (Europe/Rome) e stringa originale per delete
                try:
                    ts_str = ts if isinstance(ts, str) else str(ts)
                    # interpreta 'Z' come UTC e converte in Europe/Rome
                    dt_utc = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    ts_label = dt_utc.astimezone(ZoneInfo("Europe/Rome")).strftime("%H:%M")
                except Exception:
                    ts_label = str(ts)
                    # fallback: se manca offset, forza 'Z' per la delete
                    ts_str = ts_label + "Z" if ("Z" not in str(ts)) else str(ts)

                # Layout del pasto con bottone elimina
                col_info, col_delete = st.columns([0.85, 0.15])

                with col_info:
                    st.markdown(f"""
                    <div class="card">
                      <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div style="font-weight:700;">{meal_name}</div>
                        <div class="small">{ts_label}</div>
                      </div>
                      <div class="small" style="margin:6px 0 10px;">{notes if notes else ''}</div>
                      <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <span class="badge">🔥 {kcal} kcal</span>
                        <span class="badge">🥖 {carbs} g</span>
                        <span class="badge">🥚 {protein} g</span>
                        <span class="badge">🫒 {fat} g</span>
                      </div>
                    </div>
                    """, unsafe_allow_html=True)

                with col_delete:
                    if meal_id:
                        confirm_key = f"confirm_delete_meal_{meal_id}_{idx}"
                        if st.session_state.get(confirm_key, False):
                            if st.button("❌ Conferma", key=f"confirm_{meal_id}_{idx}",
                                         use_container_width=True, type="secondary"):
                                # Normalizza: se manca Z/offset, aggiungi Z
                                ts_to_send = ts_str
                                if "Z" not in ts_to_send and "+" not in ts_to_send:
                                    ts_to_send = ts_to_send + "Z"
                                ok, msg = delete_meal(meal_id, user_id, ts_to_send)
                                if ok:
                                    st.success(msg)
                                    st.session_state[confirm_key] = False
                                    time.sleep(5)
                                    st.rerun()
                                else:
                                    st.error(msg)
                                    st.session_state[confirm_key] = False
                            if st.button("🔙", key=f"cancel_{meal_id}_{idx}",
                                         use_container_width=True):
                                st.session_state[confirm_key] = False
                                st.rerun()
                        else:
                            if st.button("🗑️", key=f"delete_{meal_id}_{idx}",
                                         use_container_width=True):
                                st.session_state[confirm_key] = True
                                st.rerun()

        st.divider()

        st.markdown("#### 📆 Andamento (ultimi 30 giorni)")
        daily_rows = load_meals_daily(user_id, limit=30)
        if not daily_rows:
            st.info("Nessun aggregato giornaliero.")
        else:
            st.markdown('<div class="grid">', unsafe_allow_html=True)
            for d in daily_rows:
                ed = d.get("event_date")
                kcal = d.get("kcal_total", 0)
                carbs = d.get("carbs_total_g", 0)
                prot = d.get("protein_total_g", 0)
                fat = d.get("fat_total_g", 0)
                cnt = d.get("meals_count", 0)
                last = d.get("last_meal_ts")
                try:
                    last_label = datetime.fromisoformat(str(last).replace("Z", "+00:00")).astimezone(
                        ZoneInfo("Europe/Rome")).strftime("%H:%M") if last else "-"
                except Exception:
                    last_label = str(last) if last else "-"
                st.markdown(f"""
                <div class="card">
                  <div style="display:flex; justify-content:space-between;">
                    <div style="font-weight:700;">{ed}</div>
                    <div class="small">Ultimo pasto: {last_label}</div>
                  </div>
                  <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:6px;">
                    <span class="badge">🔥 {kcal} kcal</span>
                    <span class="badge">🥖 {carbs} g</span>
                    <span class="badge">🥚 {prot} g</span>
                    <span class="badge">🫒 {fat} g</span>
                    <span class="badge">🍽️ {cnt} pasti</span>
                  </div>
                </div>
                """, unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    # =========================
    # Modalità 2: Aggiungi pasto
    # =========================
    else:
        st.markdown("#### Aggiunta rapida")

        # dati dal DB
        try:
            meals_personalized_list = db.personalized_food(user_id)
        except TypeError:
            meals_personalized_list = db.personalized_food()
        meals_default_list = db.default_food()

        # tabs: ⭐ | 📚 | 🔎 | ➕
        t1, t2, t3, t4 = st.tabs(["⭐ Personalizzati", "📚 Default", "🔎 Cerca", "➕ Personalizzato"])

        with t1:
            _grid_personalized(meals_personalized_list)

        with t2:
            _grid_buttons(meals_default_list, source_label="default")

        with t3:
            c1, c2 = st.columns([3, 1])
            with c1:
                q = st.text_input("Cerca negli alimenti", placeholder="Es. pollo, pasta, yogurt")
            with c2:
                n = st.number_input("Max risultati", min_value=4, max_value=40, step=4, value=12)

            merged = _normalize_items(meals_personalized_list) + _normalize_items(meals_default_list)
            if q:
                q_low = q.lower()
                merged = [it for it in merged if q_low in it["name"].lower()]

            seen, deduped = set(), []
            for it in merged:
                key = (it["name"], it.get("quantity"), it.get("unit"))
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(it)

            foods_search = deduped[: int(n)]
            _grid_buttons(foods_search, source_label="search")

        with t4:
            st.write("Aggiungi rapidamente un alimento ai **tuoi Personalizzati** (comparirà nella tab ⭐).")


            def _nfloat(x):
                try:
                    return float(x) if x is not None else None
                except Exception:
                    return None


            def _nint(x):
                try:
                    return int(x) if x is not None else 0
                except Exception:
                    return 0


            with st.form("form_personal_food_quick", clear_on_submit=True, border=True):
                c1, c2 = st.columns([2, 1])
                with c1:
                    name_in = st.text_input("Nome alimento *", placeholder="Es. Insalata di pollo")
                with c2:
                    category_in = st.text_input("Categoria", placeholder="Es. pranzo, snack")

                q1, q2 = st.columns([1, 1])
                with q1:
                    quantity_in = st.number_input("Quantità", min_value=0.0, step=1.0, value=0.0)
                with q2:
                    unit_in = st.text_input("Unità", placeholder="g, ml, porzione…")

                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    calories_in = st.number_input("Calorie", min_value=0, step=10, value=0)
                with m2:
                    carbs_in = st.number_input("Carboidrati (g)", min_value=0, step=1, value=0)
                with m3:
                    protein_in = st.number_input("Proteine (g)", min_value=0, step=1, value=0)
                with m4:
                    fat_in = st.number_input("Grassi (g)", min_value=0, step=1, value=0)

                adv = st.expander("Altri nutrienti (opzionali)")
                with adv:
                    n1, n2, n3, n4 = st.columns(4)
                    with n1:
                        fiber_in = st.number_input("Fibre (g)", min_value=0, step=1, value=0)
                    with n2:
                        sugars_in = st.number_input("Zuccheri (g)", min_value=0, step=1, value=0)
                    with n3:
                        sat_in = st.number_input("Saturi (g)", min_value=0, step=1, value=0)
                    with n4:
                        trans_in = st.number_input("Trans (g)", min_value=0, step=1, value=0)

                    n5, n6, n7, n8 = st.columns(4)
                    with n5:
                        chol_in = st.number_input("Colesterolo (mg)", min_value=0, step=1, value=0)
                    with n6:
                        pot_in = st.number_input("Potassio (mg)", min_value=0, step=1, value=0)
                    with n7:
                        iron_in = st.number_input("Ferro (mg)", min_value=0, step=1, value=0)
                    with n8:
                        vitc_in = st.number_input("Vitamina C (mg)", min_value=0, step=1, value=0)

                    vita_in = st.number_input("Vitamina A (µg)", min_value=0, step=1, value=0)

                submitted = st.form_submit_button("Salva alimento personalizzato", use_container_width=True)
                if submitted:
                    if not name_in.strip():
                        st.error("Il nome dell'alimento è obbligatorio.")
                    else:
                        food = {
                            "name": name_in.strip(),
                            "quantity": _nfloat(quantity_in),
                            "unit": unit_in or None,
                            "calories": _nint(calories_in),
                            "carbohydrates": _nint(carbs_in),
                            "protein": _nint(protein_in),
                            "fat": _nint(fat_in),
                            "fiber": _nint(fiber_in),
                            "sugars": _nint(sugars_in),
                            "saturated_fat": _nint(sat_in),
                            "trans_fat": _nint(trans_in),
                            "cholesterol": _nint(chol_in),
                            "potassium": _nint(pot_in),
                            "iron": _nint(iron_in),
                            "vitamin_c": _nint(vitc_in),
                            "vitamin_a": _nint(vita_in),
                            "category": category_in or None,
                        }
                        try:
                            new_id = db.insert_personalized_food(user_id=user_id, food=food)
                            st.success(f"Alimento aggiunto ai Personalizzati ✅ (id: {new_id})")
                            time.sleep(2)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Errore salvataggio: {e}")

        # =========================
        # Aggiunta con data/ora (menu a tendina)
        # =========================
        st.divider()
        st.markdown("#### ⏱️ Aggiungi un pasto con giorno e ora")

        # Prepara lista unificata per selectbox (con ricerca)
        merged_all = _normalize_items(meals_personalized_list) + _normalize_items(meals_default_list)

        # dedup per (source, name, qty, unit) così distinguiamo personalizzati e default omonimi
        seen_keys = set()
        options = []
        for it in merged_all:
            key = (it.get("source") or "default", it["name"], it.get("quantity"), it.get("unit"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            star = "⭐ " if (it.get("source") == "personalized") else ""
            qty_label = f" ({it['quantity']} {it['unit']})" if it.get("quantity") and it.get("unit") else ""
            label = f"{star}{it['name']}{qty_label} — 🔥 {it.get('kcal', 0)} kcal"
            options.append((label, {"source": it.get("source") or "default", "name": it["name"]}))

        if not options:
            st.info("Non ci sono alimenti disponibili per l'inserimento manuale.")
        else:
            with st.form("form_add_meal_custom_dt", clear_on_submit=True, border=True):
                cdate, ctime = st.columns([1, 1])
                with cdate:
                    day_sel: date = st.date_input("Giorno del pasto", value=date.today(), format="YYYY-MM-DD",
                                                  key="meal_day_sel")
                with ctime:
                    now_local = datetime.now(ZoneInfo("Europe/Rome")).time().replace(second=0, microsecond=0)
                    time_sel: dtime = st.time_input("Ora del pasto", value=now_local, step=300, key="meal_time_sel")

                cfood, cnotes = st.columns([2, 1])
                with cfood:
                    label_list = [lbl for (lbl, _) in options]
                    idx_default = 0 if label_list else None
                    chosen_label = st.selectbox(
                        "Alimento (digitando filtri la lista)",
                        options=label_list,
                        index=idx_default,
                        help="Include sia i tuoi ⭐ personalizzati che gli alimenti di default."
                    )
                with cnotes:
                    notes_in = st.text_input("Note (opzionale)", placeholder="es. senza olio, doppia porzione...")

                submit_custom = st.form_submit_button("➕ Aggiungi pasto con data/ora", use_container_width=True)

                if submit_custom:
                    chosen_map = {lbl: val for (lbl, val) in options}
                    meta = chosen_map.get(chosen_label)
                    if not meta:
                        st.error("Selezione alimento non valida.")
                    else:
                        # lookup preciso su DB
                        if meta["source"] == "personalized":
                            food = db.get_personalized_food(user_id, meta["name"])
                        else:
                            food = db.get_food(meta["name"])

                        if not food:
                            st.error("Alimento non trovato nel database.")
                        else:
                            payload = {
                                "meal_id": str(uuid.uuid4()),
                                "user_id": user_id,
                                "meal_name": food["name"],
                                "kcal": int(float(food.get("calories") or 0)),
                                "carbs_g": int(float(food.get("carbohydrates") or 0)),
                                "protein_g": int(float(food.get("protein") or 0)),
                                "fat_g": int(float(food.get("fat") or 0)),
                                "timestamp": _to_iso_utc_from_local(day_sel, time_sel, tz="Europe/Rome"),
                                "notes": notes_in or "",
                                "quantity": float(food.get("quantity") or 0),
                                "unit": food.get("unit") or None,
                                "category": food.get("category") or None,
                            }
                            ok, msg = send_meal(payload)
                            if ok:
                                st.success("Pasto aggiunto ✅")
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error(msg)
