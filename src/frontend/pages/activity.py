# pages/activities.py
import os
import uuid
from datetime import datetime, date, time as dtime, timezone, timedelta
from zoneinfo import ZoneInfo
import time as _time
import streamlit as st

from utility import database_connection as db
from frontend_utility import ui  # sidebar + header + css comuni

# =========================
# Config pagina + stile
# =========================
st.set_page_config(page_title="Attività", layout="wide", initial_sidebar_state="collapsed")
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
BACKEND_URL = os.getenv("GATEWAY_URL", "http://gateway:8000")


def _backend() -> str | None:
    if not BACKEND_URL:
        return None
    return BACKEND_URL.rstrip("/")


# =========================
# Time helpers
# =========================
DEFAULT_TZ = "Europe/Rome"


def _to_iso_utc_from_local(day: date, at: dtime, tz: str = DEFAULT_TZ) -> str:
    naive = datetime.combine(day, at).replace(microsecond=0)
    local_dt = naive.replace(tzinfo=ZoneInfo(tz))
    utc_dt = local_dt.astimezone(timezone.utc)
    return utc_dt.isoformat().replace("+00:00", "Z")


def _iso_local_label(iso_ts: str | None, tz: str = DEFAULT_TZ, fmt: str = "%Y-%m-%d %H:%M") -> str:
    if not iso_ts:
        return "-"
    try:
        dt_utc = datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
        return dt_utc.astimezone(ZoneInfo(tz)).strftime(fmt)
    except Exception:
        return str(iso_ts)


def _now_local(tz: str = DEFAULT_TZ) -> datetime:
    return datetime.now(ZoneInfo(tz)).replace(second=0, microsecond=0)


# =========================
# HTTP client verso il Gateway
# =========================
def send_activity(payload: dict) -> tuple[bool, str]:
    be = _backend()
    if not be:
        return False, "Backend non configurato"
    try:
        import requests
        url = f"{be}/api/activities"
        payload["user_id"] = str(payload["user_id"])
        r = requests.post(url, json=payload, timeout=6)
        if r.ok:
            ack = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            aid = ack.get("activity_event_id") or payload.get("activity_event_id")
            return True, f"Attività inviata. id={aid}"
        return False, f"Errore backend: {r.status_code} {r.text}"
    except Exception as e:
        return False, f"Errore di rete: {e}"


def delete_activity(activity_event_id: str, user_id: str, timestamp: str) -> tuple[bool, str]:
    """Invia richiesta di delete al gateway (il TS è ignorato dal backend ma lo passiamo per retrocompatibilità)."""
    be = _backend()
    if not be:
        return False, "Backend non configurato"
    try:
        import requests
        url = f"{be}/api/activities/{activity_event_id}"
        params = {"user_id": str(user_id), "timestamp": timestamp}
        r = requests.delete(url, params=params, timeout=10)
        if r.ok:
            return True, "Attività eliminata"
        else:
            error_detail = r.json().get("detail", r.text) if r.headers.get("content-type", "").startswith("application/json") else r.text
            return False, f"Errore backend: {r.status_code} - {error_detail}"
    except Exception as e:
        return False, f"Errore di rete: {e}"


def load_activities(user_id: str, limit: int = 25, start_date: str | None = None, end_date: str | None = None) -> list[dict]:
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
        url = f"{be}/activities/facts"
        r = requests.get(url, params=params, timeout=20)
        return r.json() if r.ok else []
    except Exception:
        return []


def load_activities_daily(user_id: str, start_date: str | None = None, end_date: str | None = None, limit: int = 30) -> list[dict]:
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
        url = f"{be}/activities/daily"
        r = requests.get(url, params=params, timeout=20)
        return r.json() if r.ok else []
    except Exception:
        return []


# =========================
# Helpers UI
# =========================
def _normalize_activities(rows):
    norm = []
    for it in (rows or []):
        if isinstance(it, dict):
            norm.append({
                "activity_id": it.get("activity_id") or it.get("id"),
                "name": it.get("name") or "Attività",
                "icon": it.get("icon") or "🏃",
            })
        else:
            try:
                aid, nm, ic = it
                norm.append({"activity_id": aid, "name": nm, "icon": ic or "🏃"})
            except Exception:
                pass
    return norm


def _fmt_num(x, digits=0, dash="-"):
    try:
        if x is None:
            return dash
        return f"{float(x):.{digits}f}"
    except Exception:
        return dash


def _km_from_m(m):
    try:
        return float(m) / 1000.0 if m is not None else None
    except Exception:
        return None


def _sum_or_none(values):
    has_any = False
    acc = 0.0
    for v in values:
        if v is not None:
            has_any = True
            acc += float(v)
    return acc if has_any else None


def _avg_or_none(values):
    nums = [float(v) for v in values if v is not None]
    return (sum(nums) / len(nums)) if nums else None


# =========================
# Layout a 2 colonne
# =========================
sidebar_col, main_col = st.columns([0.8, 6.2], gap="large")

with sidebar_col:
    ui.render_sidebar(name, surname, user_id)

with main_col:
    ui.render_header("Attività", "Registra allenamenti e visualizza la cronologia.")

    mode = st.radio(
        "Seleziona se vuoi vedere le attività del giorno o aggiungerne una nuova",
        options=["📆 Attività del giorno", "➕ Aggiungi / Registra"],
        horizontal=True,
        index=0,
    )

    # =========================
    # Modalità 1: Attività del giorno (FACT + DAILY)
    # =========================
    if mode == "📆 Attività del giorno":
        c1, c2 = st.columns([1.2, 0.3])
        with c1:
            sel_date: date = st.date_input("Giorno", value=date.today(), format="YYYY-MM-DD")
        with c2:
            refresh = st.button("🔄 Ricarica", use_container_width=True)

        day_str = sel_date.strftime("%Y-%m-%d")
        acts_day = load_activities(user_id, limit=500, start_date=day_str, end_date=day_str)

        # Riepilogo giornaliero:
        # - conteggio/durata/ultima fine da DAILY (quadro di controllo semplice)
        # - kcal/steps/distanza/HR calcolati dai FACT del giorno (dato che il DAILY non li contiene più)
        daily_rows = load_activities_daily(user_id, start_date=day_str, end_date=day_str, limit=1)
        duration_total_min = (daily_rows[0].get("duration_total_min") if daily_rows else None) or 0
        activities_count = (daily_rows[0].get("activities_count") if daily_rows else None) or 0
        last_activity_end_ts = daily_rows[0].get("last_activity_end_ts") if daily_rows else None
        last_label = _iso_local_label(last_activity_end_ts)

        kcal_day = _sum_or_none([a.get("calories_total") for a in acts_day])
        steps_day = _sum_or_none([a.get("steps_total") for a in acts_day])
        dist_m_day = _sum_or_none([a.get("distance_m") for a in acts_day])
        hr_day = _avg_or_none([a.get("hr_bpm_avg") for a in acts_day])

        # --- RIEPILOGO GIORNALIERO (SOLO DAILY) ---
        st.markdown("#### Riepilogo giornaliero")

        daily_rows = load_activities_daily(user_id, start_date=day_str, end_date=day_str, limit=1)
        duration_total_min = (daily_rows[0].get("duration_total_min") if daily_rows else None) or 0
        activities_count = (daily_rows[0].get("activities_count") if daily_rows else None) or 0
        last_activity_end_ts = daily_rows[0].get("last_activity_end_ts") if daily_rows else None
        last_label = _iso_local_label(last_activity_end_ts)

        st.markdown(f"""
        <div class="card" style="display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
            <div style="font-weight:700;">{day_str}</div>
            <div class="badge">🏷️ {activities_count} attività</div>
            <div class="badge">⏱️ {_fmt_num(duration_total_min, 0)} min</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("#### Dettaglio attività del giorno")
        if not acts_day:
            st.info("Nessuna attività registrata per il giorno selezionato.")
        else:
            for idx, a in enumerate(acts_day):
                aeid = a.get("activity_event_id") or a.get("id")
                name_a = a.get("activity_name") or a.get("name") or "Attività"
                icon = a.get("icon") or "🏃"
                start_ts = a.get("start_ts") or a.get("start")
                end_ts = a.get("end_ts") or a.get("end")
                dur = float(a.get("duration_min") or 0)
                notes = a.get("notes", "")

                start_label = _iso_local_label(start_ts)
                end_label = _iso_local_label(end_ts)

                # KPI dal FACT
                cal = a.get("calories_total")
                steps = a.get("steps_total")
                hr = a.get("hr_bpm_avg")
                dist_m = a.get("distance_m")
                pace_m_min = a.get("pace_m_per_min")

                badges_html = f'<span class="badge">⏱️ {_fmt_num(dur, 0)} min</span>'
                if cal is not None:
                    badges_html += f'<span class="badge">🔥 {_fmt_num(cal, 0)} kcal</span>'
                if steps is not None:
                    badges_html += f'<span class="badge">👟 {_fmt_num(steps, 0)} passi</span>'
                if dist_m is not None:
                    badges_html += f'<span class="badge">📏 {_fmt_num(_km_from_m(dist_m), 2)} km</span>'
                if pace_m_min is not None:
                    badges_html += f'<span class="badge">🐇 {_fmt_num(pace_m_min, 0)} m/min</span>'
                if hr is not None:
                    badges_html += f'<span class="badge">❤️ {_fmt_num(hr, 0)} bpm</span>'

                col_info, col_delete = st.columns([0.85, 0.15])
                with col_info:
                    st.markdown(f"""
                    <div class="card">
                      <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div style="font-weight:700;">{icon} {name_a}</div>
                        <div class="small">{start_label} – {end_label}</div>
                      </div>
                      <div class="small" style="margin:6px 0 10px;">{notes if notes else ''}</div>
                      <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        {badges_html}
                      </div>
                    </div>
                    """, unsafe_allow_html=True)

                with col_delete:
                    if aeid:
                        confirm_key = f"confirm_delete_activity_{aeid}_{idx}"
                        ts_for_delete = end_ts or start_ts
                        if st.session_state.get(confirm_key, False):
                            if st.button("❌ Conferma", key=f"confirm_{aeid}_{idx}", use_container_width=True, type="secondary"):
                                ts_to_send = str(ts_for_delete) if ts_for_delete else ""
                                if ts_to_send and ("Z" not in ts_to_send and "+" not in ts_to_send):
                                    ts_to_send = ts_to_send + "Z"
                                ok, msg = delete_activity(aeid, user_id, ts_to_send)
                                if ok:
                                    st.success(msg)
                                    st.session_state[confirm_key] = False
                                    _time.sleep(2)
                                    st.rerun()
                                else:
                                    st.error(msg)
                                    st.session_state[confirm_key] = False
                            if st.button("🔙", key=f"cancel_{aeid}_{idx}", use_container_width=True):
                                st.session_state[confirm_key] = False
                                st.rerun()
                        else:
                            if st.button("🗑️", key=f"delete_{aeid}_{idx}", use_container_width=True):
                                st.session_state[confirm_key] = True
                                st.rerun()

        st.divider()

        st.markdown("#### 📆 Andamento (ultimi 30 giorni)")
        daily_30 = load_activities_daily(user_id, limit=30)
        if not daily_30:
            st.info("Nessun aggregato giornaliero.")
        else:
            st.markdown('<div class="grid">', unsafe_allow_html=True)
            for d in daily_30:
                ed = d.get("event_date")
                dur = d.get("duration_total_min", 0)
                cnt = d.get("activities_count", 0)
                last = d.get("last_activity_end_ts") or d.get("last_end_ts")
                last_label = _iso_local_label(last)

                # Mostra SOLO i campi disponibili nel DAILY
                st.markdown(f"""
                <div class="card">
                  <div style="display:flex; justify-content:space-between;">
                    <div style="font-weight:700;">{ed}</div>
                    <div class="small">Ultima: {last_label}</div>
                  </div>
                  <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:6px;">
                    <span class="badge">🏷️ {cnt} attività</span>
                    <span class="badge">⏱️ {_fmt_num(dur, 0)} min</span>
                  </div>
                </div>
                """, unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    # =========================
    # Modalità 2: Aggiungi / Registra (Quick + Manuale)
    # =========================
    else:
        st.markdown("#### ➕ Aggiungi / Registra attività")

        # Dati dal DB
        try:
            activities_default_list = db.activity_default()
        except Exception:
            activities_default_list = []

        items = _normalize_activities(activities_default_list)
        options = [f"{it['icon']} {it['name']}" for it in items]
        id_by_label = {f"{it['icon']} {it['name']}": it["activity_id"] for it in items}
        name_by_label = {f"{it['icon']} {it['name']}": it["name"] for it in items}

        t1, t2 = st.tabs(["⭐ Aggiunta rapida", "📝 Registra attività"])

        # ---------- Aggiunta rapida ----------
        with t1:
            if not options:
                st.info("Non ci sono attività disponibili.")
            else:
                qc1, qc2, qc3 = st.columns([1, 1, 1])
                with qc1:
                    quick_day: date = st.date_input("Data (rapida)", value=date.today(), format="YYYY-MM-DD", key="quick_day")
                with qc2:
                    anchor = st.selectbox("Ancoraggio", ["🕒 Finita ora"], index=0, key="quick_anchor")
                with qc3:
                    duration_min = st.number_input("Durata (min)", min_value=1, max_value=600, step=5, value=30, key="quick_dur")

                # Ora locale corrente (min granularity)
                now_local = _now_local(DEFAULT_TZ).time()

                # Griglia attività
                cols = st.columns(3)
                for i, label in enumerate(options):
                    col = cols[i % 3]
                    with col:
                        st.markdown(
                            f'<div class="card" style="text-align:center; font-weight:600;">{label}</div>',
                            unsafe_allow_html=True
                        )
                        if st.button("➕ Aggiungi", use_container_width=True, key=f"q_add_{i}"):
                            # Calcolo start/end
                            if anchor.startswith("🕒"):
                                # Finita ora -> end = now, start = end - durata
                                end_iso = _to_iso_utc_from_local(quick_day, now_local, tz=DEFAULT_TZ)
                                end_dt_local = datetime.combine(quick_day, now_local).replace(tzinfo=ZoneInfo(DEFAULT_TZ))
                                start_dt_local = end_dt_local - timedelta(minutes=int(duration_min))
                                start_iso = start_dt_local.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                            else:
                                # Iniziata ora -> start = now, end = start + durata
                                start_iso = _to_iso_utc_from_local(quick_day, now_local, tz=DEFAULT_TZ)
                                start_dt_local = datetime.combine(quick_day, now_local).replace(tzinfo=ZoneInfo(DEFAULT_TZ))
                                end_dt_local = start_dt_local + timedelta(minutes=int(duration_min))
                                end_iso = end_dt_local.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

                            aid = id_by_label.get(label)
                            aname = name_by_label.get(label)
                            payload = {
                                "activity_event_id": str(uuid.uuid4()),
                                "user_id": user_id,
                                "activity_id": int(aid),
                                "activity_name": aname,
                                "start_ts": start_iso,
                                "end_ts": end_iso,
                                "notes": f"quick add ({anchor.lower()})",
                            }
                            ok, msg = send_activity(payload)
                            if ok:
                                st.success("Attività registrata ✅")
                                _time.sleep(2)
                                st.rerun()
                            else:
                                st.error(msg)

        # ---------- Registra attività (manuale con data/ora al minuto) ----------
        with t2:
            if not options:
                st.info("Non ci sono attività disponibili.")
            else:
                with st.form("form_add_activity_custom", clear_on_submit=True, border=True):
                    c1, c2 = st.columns([1, 1])
                    with c1:
                        day_start: date = st.date_input("Data inizio", value=date.today(), format="YYYY-MM-DD")
                        time_start: dtime = st.time_input("Ora inizio", value=_now_local(DEFAULT_TZ).time(), step=60)
                    with c2:
                        day_end: date = st.date_input("Data fine", value=date.today(), format="YYYY-MM-DD")
                        time_end: dtime = st.time_input("Ora fine", value=_now_local(DEFAULT_TZ).time(), step=60)

                    cact, cnotes = st.columns([2, 1])
                    with cact:
                        chosen_label = st.selectbox("Attività", options=options)
                    with cnotes:
                        notes_in = st.text_input("Note (opzionale)")

                    submit_custom = st.form_submit_button("➕ Registra attività", use_container_width=True)

                    if submit_custom:
                        aid = id_by_label.get(chosen_label)
                        aname = name_by_label.get(chosen_label)

                        if not aid:
                            st.error("Selezione attività non valida.")
                        else:
                            start_iso = _to_iso_utc_from_local(day_start, time_start, tz=DEFAULT_TZ)
                            end_iso = _to_iso_utc_from_local(day_end, time_end, tz=DEFAULT_TZ)

                            # Validazione: end >= start
                            start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
                            end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                            if end_dt < start_dt:
                                st.error("L'ora di fine deve essere successiva all'ora di inizio.")
                            else:
                                payload = {
                                    "activity_event_id": str(uuid.uuid4()),
                                    "user_id": user_id,
                                    "activity_id": int(aid),
                                    "activity_name": aname,
                                    "start_ts": start_iso,
                                    "end_ts": end_iso,
                                    "notes": notes_in or "",
                                }
                                ok, msg = send_activity(payload)
                                if ok:
                                    st.success("Attività registrata ✅")
                                    _time.sleep(2)
                                    st.rerun()
                                else:
                                    st.error(msg)
