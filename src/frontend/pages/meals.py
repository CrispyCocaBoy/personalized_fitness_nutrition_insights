# pages/meals.py
import os
import uuid
from datetime import datetime
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
BACKEND_URL = os.getenv("BACKEND_URL")

def send_meal(payload: dict) -> tuple[bool, str]:
    if BACKEND_URL:
        try:
            import requests
            r = requests.post(f"{BACKEND_URL}/api/meals", json=payload, timeout=6)
            if r.ok:
                return True, "Pasto inviato al backend."
            return False, f"Errore backend: {r.status_code} {r.text}"
        except Exception as e:
            return False, f"Errore di rete: {e}"
    try:
        db.insert_meal(
            user_id=payload["user_id"],
            meal_id=payload["meal_id"],
            ts=payload["timestamp"],
            name=payload["meal_name"],
            kcal=payload["kcal"],
            carbs=payload["carbs_g"],
            protein=payload["protein_g"],
            fat=payload["fat_g"],
            notes=payload.get("notes", "")
        )
        return True, "Pasto salvato."
    except Exception as e:
        return False, f"Errore DB: {e}"

def load_meals(limit: int = 20):
    if BACKEND_URL:
        try:
            import requests
            r = requests.get(f"{BACKEND_URL}/api/meals", params={"user_id": user_id, "limit": limit}, timeout=6)
            if r.ok:
                return r.json()
            else:
                st.warning(f"Impossibile leggere dal backend: {r.status_code}")
        except Exception as e:
            st.warning(f"Errore di rete: {e}")
    try:
        return db.get_meals(user_id=user_id, limit=limit)
    except Exception as e:
        st.warning(f"Errore DB: {e}")
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
                    "name": it, "kcal": 0, "carbs_g": 0, "protein_g": 0, "fat_g": 0,
                    "quantity": None, "unit": None, "food_id": None
                })
            elif isinstance(it, dict):
                norm.append({
                    "name": it.get("name") or "Alimento",
                    "kcal": int(float(it.get("kcal") or it.get("calories") or 0)),
                    "carbs_g": int(float(it.get("carbs_g") or 0)),
                    "protein_g": int(float(it.get("protein_g") or 0)),
                    "fat_g": int(float(it.get("fat_g") or 0)),
                    "quantity": it.get("quantity"),
                    "unit": it.get("unit"),
                    "food_id": it.get("food_id") or it.get("id")
                })
        return norm

    def _grid_buttons(items, source_label: str, cols=3):
        items = _normalize_items(items)
        if not items:
            st.info("Nessun alimento disponibile.")
            return
        for i in range(0, len(items), cols):
            row = st.columns(cols)
            for col, item in zip(row, items[i:i+cols]):
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
                    if st.button("➕ Aggiungi", use_container_width=True, key=f"qa_{source_label}_{i}_{title}"):
                        payload = {
                            "meal_id": str(uuid.uuid4()),
                            "user_id": user_id,
                            "meal_name": title,
                            "kcal": item["kcal"],
                            "carbs_g": item["carbs_g"],
                            "protein_g": item["protein_g"],
                            "fat_g": item["fat_g"],
                            "timestamp": datetime.utcnow().isoformat(),
                            "notes": f"quick add ({source_label})"
                        }
                        ok, msg = send_meal(payload)
                        if ok:
                            st.success("Pasto aggiunto ✅")
                            st.experimental_rerun()
                        else:
                            st.error(msg)

    # dati dal DB
    try:
        meals_personalized_list = db.personalized_food(user_id)
    except TypeError:
        meals_personalized_list = db.personalized_food()
    meals_default_list = db.default_food()

    # tabs
    t1, t2, t3 = st.tabs(["⭐ Personalizzati", "📚 Default", "🔎 Cerca"])
    with t1:
        _grid_buttons(meals_personalized_list, source_label="personalized")
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

    st.divider()

    # ---------- Ultimi pasti ----------
    st.markdown("#### Ultimi pasti")
    meals = load_meals(limit=25)

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
