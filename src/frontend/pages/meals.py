# pages/meals.py
import os
import uuid
from datetime import datetime
import streamlit as st
from utility import database_connection as db
from frontend_utility import ui  # sidebar + header + css comuni
import time

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

def send_meal(payload: dict) -> tuple[bool, str]:
    """
    Payload atteso dal gateway:
    {
      "meal_id": str, "user_id": str, "meal_name": str,
      "kcal": int, "carbs_g": int, "protein_g": int, "fat_g": int,
      "timestamp": ISO8601 string (UTC, Z), "notes": str
    }
    """
    be = _backend()
    try:
        import requests
        url = f"{be}/api/meals"
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



def load_meals(user_id: str, limit: int = 25) -> list[dict]:
    """
    Recupera gli ultimi pasti dal backend gateway via API REST.
    """
    be = _backend()
    if not be:
        return []
    try:
        import requests
        url = f"{be}/api/meals"
        r = requests.get(url, params={"user_id": str(user_id), "limit": int(limit)}, timeout=20)
        if r.ok:
            return r.json()
        else:
            st.warning(f"Errore backend: {r.status_code} {r.text}")
            return []
    except Exception as e:
        st.warning(f"Errore di rete: {e}")
        return []

# =========================
# Layout a 2 colonne
# =========================
sidebar_col, main_col = st.columns([0.9, 6.1], gap="large")

with sidebar_col:
    backend_mode = "HTTP" if BACKEND_URL else "DB locale"
    ui.render_sidebar(name, surname, user_id)

with main_col:
    ui.render_header("Pasti", "Registra rapidamente i pasti e monitora calorie/macros.")

    # ---------- Quick add ----------
    st.markdown("#### Aggiunta rapida")

    def _normalize_items(items):
        norm = []
        for it in (items or []):
            if isinstance(it, str):
                norm.append({
                    "user_food_id": None,  # nessun ID per default list
                    "name": it, "kcal": 0, "carbs_g": 0, "protein_g": 0, "fat_g": 0,
                    "quantity": None, "unit": None,
                })
            elif isinstance(it, dict):
                norm.append({
                    # prova in quest'ordine: user_food_id (personalizzati), id, food_id
                    "user_food_id": it.get("user_food_id") or it.get("id") or it.get("food_id"),
                    "name": it.get("name") or "Alimento",
                    "kcal": int(float(it.get("kcal") or it.get("calories") or 0)),
                    "carbs_g": int(float(it.get("carbs_g") or it.get("carbohydrates") or 0)),
                    "protein_g": int(float(it.get("protein_g") or it.get("protein") or 0)),
                    "fat_g": int(float(it.get("fat_g") or it.get("fat") or 0)),
                    "quantity": it.get("quantity"),
                    "unit": it.get("unit"),
                    "category": it.get("category") or None,
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

                # Riga informativa: kcal + macro
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
                        # 🔎 prendo dal DB in base alla fonte
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
                            # 🔎 prendo TUTTI i campi dal DB (personalizzati)
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
                                    # extra informativi (non usati da Spark ma utili da tenere)
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
                            # mini conferma: doppio click (usa session_state)
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

    # dati dal DB
    try:
        meals_personalized_list = db.personalized_food(user_id)
    except TypeError:
        meals_personalized_list = db.personalized_food()
    meals_default_list = db.default_food()

    # tabs
    t1, t2, t3, t4 = st.tabs(["⭐ Personalizzati", "📚 Default", "🔎 Cerca","➕ Personalizzato"])
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

        # Merge liste e normalizza
        merged = _normalize_items(meals_personalized_list) + _normalize_items(meals_default_list)

        # Applica filtro testo
        if q:
            q_low = q.lower()
            merged = [it for it in merged if q_low in it["name"].lower()]

        # Deduplica per nome+qty+unit
        seen, deduped = set(), []
        for it in merged:
            key = (it["name"], it.get("quantity"), it.get("unit"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(it)

        # Limita ai primi n risultati
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
                        st.rerun()  # lo rende subito visibile nella tab ⭐
                    except Exception as e:
                        st.error(f"Errore salvataggio: {e}")

    st.divider()

    # ---------- Ultimi pasti ----------
    st.markdown("#### Ultimi pasti")
    meals = load_meals(user_id, limit=25)

    if not meals:
        st.info("Nessun pasto registrato di recente.")
    else:
        st.markdown('<div class="grid">', unsafe_allow_html=True)
        for m in meals:
            meal_name = m.get("meal_name") or m.get("name") or "Pasto"
            ts = m.get("timestamp") or m.get("ts")
            kcal = m.get("kcal", 0)
            carbs = m.get("carbs_g", 0)
            protein = m.get("protein_g", 0)
            fat = m.get("fat_g", 0)
            notes = m.get("notes", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z",""))
                ts_label = dt.strftime("%d %b %Y, %H:%M")
            except Exception:
                ts_label = ts
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
        st.markdown('</div>', unsafe_allow_html=True)
