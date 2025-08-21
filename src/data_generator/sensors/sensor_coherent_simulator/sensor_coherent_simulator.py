from paho.mqtt import client as mqtt_client
import json
import time
import logging
import socket
import math
import pandas as pd

# =========================
# CONFIG PATHS (CSV)
# =========================
events_path = "sample_data/events_ALLusers.csv"
device_path = "sample_data/device.csv"
sensor_to_user_path = "sample_data/sensor_to_user.csv"
feature_path = "sample_data/feature.csv"  # non usato direttamente ma tenuto per compatibilità

# =========================
# MQTT configuration
# =========================
broker = 'emqx1'
port = 1883
client_id = "csv_raw_simulator"
username = 'emqx'
password = 'public'

# Topics / unità
RAW_TOPIC = {
    "ppg":            "wearables/PPG",
    "accelerometer":  "wearables/Accelerometer",
    "ceda":           "wearables/cEDA",
    "skin_temp":      "wearables/SkinTemp",
}
RAW_UNIT = {"ppg": "adc", "accelerometer": "g", "ceda": "uS", "skin_temp": "C"}

# =========================
# Frequenze (Hz)
# =========================
PPG_FS = 5          # era 100
ACC_FS = 5          # era 50
CEDA_FS = 1         # era 4
SKIN_FS = 1         # generiamo a 1 Hz ma pubblichiamo 1/5 s (vedi sotto)
SKIN_EVERY_N_SECONDS = 5  # → 0.2 Hz effettivi

# --- Playhead & gating ---
PLAYBACK_SPEED = 0.5
LOOP_DATASET = True

# Parti da un istante sicuramente "attivo"
PLAYHEAD_START_MODE = "first_active"   # "csv_start" | "first_active" | "custom"
CUSTOM_PLAYHEAD_START = None           # es: "2025-05-15 12:00:00" se usi "custom"

# gating (per test, puoi metterli a False)
REQUIRE_EVENT = False          # se False, pubblica anche senza evento
ENFORCE_START_ACTIVE = True   # se False, ignora start_active

# =========================
# Reconnection strategy
# =========================
FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60
STARTUP_MAX_RETRIES = 30
STARTUP_RETRY_DELAY = 2

# =========================
# GLOBALS (settati in run())
# =========================
SENSORS = None
EVENTS = None
CSV_START = None
CSV_END = None
PLAYHEAD_BASE = None
SENSOR_STATES = None
SENSOR_META = None
WALLCLOCK_T0 = None

# =========================
# LOAD CSVs
# =========================
# Lista utenti che vuoi simulare
ALLOWED_USERS = [1, 2, 5]

def load_data():
    events = pd.read_csv(events_path, dtype={"user_id":"int64","activity":"string"})
    events["start"] = pd.to_datetime(events["start"])
    events["end"]   = pd.to_datetime(events["end"])

    devices = pd.read_csv(device_path, dtype={"device_id":"int64","user_id":"int64"})
    if "registered_at" not in devices.columns:
        raise ValueError("device.csv deve includere la colonna 'registered_at'.")
    devices["registered_at"] = pd.to_datetime(devices["registered_at"])

    s2u = pd.read_csv(sensor_to_user_path, dtype={
        "sensor_id":"int64",
        "user_id":"int64",
        "device_id":"int64",
        "sensor_type_id":"int64"
    })
    s2u["created_at"] = pd.to_datetime(s2u["created_at"])

    _ = pd.read_csv(feature_path)  # non utilizzato direttamente

    # --- FILTRO UTENTI QUI ---
    s2u = s2u[s2u["user_id"].isin(ALLOWED_USERS)]
    events = events[events["user_id"].isin(ALLOWED_USERS)]

    sensors = s2u.merge(
        devices[["device_id","device_type_id","registered_at"]],
        on="device_id", how="left", validate="many_to_one"
    )

    # Mappa tipi sensore -> raw_kind (1: HR/PPG, 2: Skin, 3: Accelerometer, 4: cEDA)
    sensor_type_to_raw = {1:"ppg", 2:"skin_temp", 3:"accelerometer", 4:"ceda"}
    sensors["raw_kind"] = sensors["sensor_type_id"].map(sensor_type_to_raw)
    sensors = sensors.dropna(subset=["user_id","raw_kind"]).copy()
    sensors["user_id"] = sensors["user_id"].astype(int)
    sensors["start_active"] = sensors[["registered_at","created_at"]].max(axis=1)
    sensors = sensors.drop_duplicates(subset=["sensor_id","raw_kind"]).reset_index(drop=True)

    events = events.sort_values(["user_id","start"]).reset_index(drop=True)
    csv_start = events["start"].min()
    csv_end   = events["end"].max()
    return sensors, events, csv_start, csv_end

# =========================
# MQTT callbacks
# =========================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Connected to MQTT Broker successfully")
        print("Connected to MQTT Broker")
    else:
        logging.error(f"Failed to connect to MQTT Broker, return code {rc}")

def on_disconnect(client, userdata, rc):
    logging.info("Disconnected from MQTT Broker with result code: %s", rc)
    reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
    while reconnect_count < MAX_RECONNECT_COUNT:
        logging.info("Attempting reconnection in %d seconds... (attempt %d/%d)",
                     reconnect_delay, reconnect_count + 1, MAX_RECONNECT_COUNT)
        time.sleep(reconnect_delay)
        try:
            client.reconnect()
            logging.info("Reconnected successfully")
            return
        except Exception as err:
            logging.error("Reconnection attempt failed: %s. Retrying...", err)
        reconnect_delay = min(reconnect_delay * RECONNECT_RATE, MAX_RECONNECT_DELAY)
        reconnect_count += 1
    logging.critical("Reconnect failed after %d attempts. Exiting...")

# =========================
# Utility CSV: evento attivo
# =========================
def get_active_event_for_user(user_id: int, playhead_ts: pd.Timestamp):
    evu = EVENTS[EVENTS["user_id"] == user_id]
    if evu.empty:
        return None
    hit = evu[(evu["start"] <= playhead_ts) & (evu["end"] > playhead_ts)]
    if hit.empty:
        return None
    return hit.iloc[0].to_dict()

# =========================
# Generatori stateful
# =========================
class SensorState:
    __slots__ = ("sensor_id","user_id","raw_kind","fs","last_tick_ms",
                 "ppg_phase","ppg_hr_hz","ppg_rr_std","ppg_spo2",
                 "acc_phase","ceda_tonic","rng","start_active")

    def __init__(self, sensor_id, user_id, raw_kind, fs, start_active, seed=123):
        import random
        self.sensor_id = int(sensor_id)
        self.user_id = int(user_id)
        self.raw_kind = raw_kind
        self.fs = int(fs)
        self.start_active = pd.to_datetime(start_active)
        self.last_tick_ms = None
        # PPG
        self.ppg_phase = 0.0
        self.ppg_hr_hz = 75/60.0
        self.ppg_rr_std = 0.02  # ~20 ms
        self.ppg_spo2 = 97.0
        # ACC
        self.acc_phase = 0.0
        # cEDA
        self.ceda_tonic = 5.0
        # RNG
        self.rng = random.Random(seed)

    @staticmethod
    def _ppg_wave(phi):
        syst = math.exp(-0.5*((phi-0.15)/0.06)**2)
        diast = math.exp(-max(0.0, (phi-0.25))/0.35)
        notch = 0.15 * math.exp(-0.5*((phi-0.45)/0.03)**2)
        return max(0.0, 1.1*syst + 0.6*diast + notch)

    def _tick_ppg(self, dt_s, hr_mean, hrv_std_ms, spo2_mean):
        if hr_mean and hr_mean > 0:
            self.ppg_hr_hz = float(hr_mean)/60.0
        if hrv_std_ms is not None:
            self.ppg_rr_std = max(0.0, float(hrv_std_ms)/1000.0)
        if spo2_mean is not None:
            self.ppg_spo2 = float(spo2_mean)

        # leggero wandering per realismo
        self.ppg_hr_hz = max(0.6, min(3.0, self.ppg_hr_hz + self.rng.gauss(0, self.ppg_rr_std*0.02)))
        self.ppg_phase = (self.ppg_phase + self.ppg_hr_hz*dt_s) % 1.0

        resp = 1.0 + 0.05 * math.sin(2*math.pi*0.25*time.time())
        w = self._ppg_wave(self.ppg_phase) * resp

        A, B = 110.0, 25.0  # SpO2 ≈ A - B*R
        R = max(0.2, min(1.2, (A - self.ppg_spo2)/B))
        ac_ir, dc_ir = 0.8, 0.2
        ac_red, dc_red = R*ac_ir, 0.2
        ir  = dc_ir  + ac_ir  * w + self.rng.gauss(0, 0.004)
        red = dc_red + ac_red * w + self.rng.gauss(0, 0.004)
        return ir, red

    def _tick_acc(self, dt_s, steps_lambda_5s):
        import random as pyrand
        steps_per_s = max(0.0, (steps_lambda_5s or 0.0) / 5.0)
        gait_hz = min(3.0, steps_per_s / 2.0)
        self.acc_phase = (self.acc_phase + gait_hz * dt_s) % 1.0

        x = 0.25 * math.sin(2 * math.pi * self.acc_phase + 1.3) + pyrand.gauss(0, 0.05)
        y = 0.20 * math.sin(2 * math.pi * self.acc_phase + 0.4) + pyrand.gauss(0, 0.05)
        z = (
                1.0
                + 0.60 * math.sin(2 * math.pi * self.acc_phase)
                + 0.30 * math.sin(4 * math.pi * self.acc_phase + 0.8)
                + pyrand.gauss(0, 0.05)
        )
        return round(x, 5), round(y, 5), round(z, 5)

    def _tick_ceda(self, scl_mean, scl_std):
        import random as pyrand
        base = float(scl_mean or 5.0)
        self.ceda_tonic = max(1.0, min(25.0, base + pyrand.gauss(0, 0.02)))
        if pyrand.random() < 0.02:
            self.ceda_tonic = min(25.0, self.ceda_tonic + abs(pyrand.gauss(0.4,0.1)))
        return round(self.ceda_tonic + pyrand.gauss(0, 0.02), 4)

    def _tick_skin(self, skin_mean, skin_std):
        import random as pyrand
        base = float(skin_mean or 33.5)
        val = base + pyrand.gauss(0, float(skin_std or 0.2)*0.05) + pyrand.gauss(0, 0.02)
        return round(val,3)

    def due_samples(self, now_ms, playhead_ts):
        out = []
        if self.last_tick_ms is None:
            self.last_tick_ms = now_ms
            return out

        # gate start_active
        if ENFORCE_START_ACTIVE and playhead_ts < self.start_active:
            self.last_tick_ms = now_ms
            return out

        period_ms = int(1000/self.fs)
        while self.last_tick_ms + period_ms <= now_ms:
            self.last_tick_ms += period_ms
            ts = self.last_tick_ms
            dt_s = period_ms/1000.0

            # evento attivo per l'utente (parametri)
            ev = get_active_event_for_user(self.user_id, playhead_ts)
            if REQUIRE_EVENT and ev is None:
                continue
            if ev is None:
                ev = {}

            if self.raw_kind == "ppg":
                ir, red = self._tick_ppg(dt_s, ev.get("hr_mean"), ev.get("hrv_std"), ev.get("spo2_mean"))
                out.append(("ppg_ir", ts, ir))
                out.append(("ppg_red", ts, red))

            elif self.raw_kind == "accelerometer":
                ax, ay, az = self._tick_acc(dt_s, ev.get("steps_lambda"))
                out.append(("acc_x", ts, ax))
                out.append(("acc_y", ts, ay))
                out.append(("acc_z", ts, az))

            elif self.raw_kind == "ceda":
                eda = self._tick_ceda(ev.get("scl_mean"), ev.get("scl_std"))
                out.append(("ceda", ts, eda))

            elif self.raw_kind == "skin_temp":
                # Pubblica 1 campione ogni N secondi => 0.2 Hz effettivi con N=5
                if (ts // 1000) % SKIN_EVERY_N_SECONDS != 0:
                    # salta questo tick per ridurre i messaggi
                    continue
                temp = self._tick_skin(ev.get("skin_mean"), ev.get("skin_std"))
                out.append(("skin_temp", ts, temp))

        return out

# Costruisci stati per tutti i sensori noti dai CSV
def build_states():
    states = {}
    meta = {}
    for _, row in SENSORS.iterrows():
        raw_kind = row["raw_kind"]
        fs = {"ppg":PPG_FS, "accelerometer":ACC_FS, "ceda":CEDA_FS, "skin_temp":SKIN_FS}[raw_kind]
        sid = int(row["sensor_id"])
        st = SensorState(
            sensor_id=sid,
            user_id=int(row["user_id"]),
            raw_kind=raw_kind,
            fs=fs,
            start_active=row["start_active"],
            seed=(hash((sid, raw_kind)) & 0xffffffff)
        )
        states[sid] = st
        meta[sid] = (raw_kind, fs, int(row["user_id"]))
    return states, meta

# =========================
# Playhead controller
# =========================
def compute_playhead_base():
    base = CSV_START
    if PLAYHEAD_START_MODE == "first_active":
        first_sensor_active = SENSORS["start_active"].min()
        if pd.isna(first_sensor_active):
            first_sensor_active = CSV_START
        base = max(CSV_START, first_sensor_active)
        ev_after = EVENTS[EVENTS["end"] > base]
        if not ev_after.empty:
            first_event_after = ev_after["start"].min()
            if pd.notna(first_event_after) and first_event_after > base:
                base = first_event_after
    elif PLAYHEAD_START_MODE == "custom" and CUSTOM_PLAYHEAD_START:
        try:
            base = pd.Timestamp(CUSTOM_PLAYHEAD_START)
        except Exception:
            base = CSV_START
    # clamp
    if base < CSV_START: base = CSV_START
    if base > CSV_END:   base = CSV_END
    return base

def get_playhead_ts():
    elapsed = (time.time() - WALLCLOCK_T0) * PLAYBACK_SPEED
    ph = PLAYHEAD_BASE + pd.to_timedelta(elapsed, unit="s")
    if ph > CSV_END:
        if LOOP_DATASET:
            span = (CSV_END - PLAYHEAD_BASE).total_seconds()
            if span <= 0:
                return PLAYHEAD_BASE
            loops = int(elapsed // span)
            rem = elapsed - loops*span
            return PLAYHEAD_BASE + pd.to_timedelta(rem, unit="s")
        else:
            return CSV_END
    return ph

# =========================
# MQTT helpers
# =========================
def wait_for_mqtt_broker():
    for attempt in range(1, STARTUP_MAX_RETRIES + 1):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((broker, port))
            sock.close()
            if result == 0:
                return True
        except Exception:
            pass
        if attempt < STARTUP_MAX_RETRIES:
            time.sleep(min(STARTUP_RETRY_DELAY * attempt, 30))
    return False

def connect_mqtt_with_retry():
    for attempt in range(1, MAX_RECONNECT_COUNT + 1):
        try:
            client = mqtt_client.Client(
                client_id=client_id,
                callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2
            )
            client.username_pw_set(username, password)
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.loop_start()
            client.connect(broker, port, keepalive=60)
            t0 = time.time()
            while not client.is_connected() and time.time()-t0 < 10:
                time.sleep(0.1)
            if client.is_connected():
                return client
            client.loop_stop(); client.disconnect()
        except Exception:
            try:
                client.loop_stop(); client.disconnect()
            except:
                pass
        if attempt < MAX_RECONNECT_COUNT:
            time.sleep(min(FIRST_RECONNECT_DELAY * (RECONNECT_RATE ** (attempt - 1)), MAX_RECONNECT_DELAY))
    return None

# =========================
# Publisher
# =========================
def publish_tick(client):
    now_ms = int(time.time()*1000)
    playhead_ts = get_playhead_ts()
    published = 0
    for sid, st in SENSOR_STATES.items():
        if st.last_tick_ms is None:
            st.last_tick_ms = now_ms

        due = st.due_samples(now_ms, playhead_ts)
        if not due:
            continue

        raw_kind, _, _ = SENSOR_META[sid]
        topic = RAW_TOPIC[raw_kind]
        unit = RAW_UNIT[raw_kind]

        for variable, ts, value in due:
            payload = {
                "sensor_id": sid,
                "timestamp": ts,       # ms real-time
                "metric": variable,
                "value": value,
            }
            res = client.publish(topic, json.dumps(payload), qos=0)
            if res.rc == mqtt_client.MQTT_ERR_SUCCESS:
                published += 1
    return published

def publish_loop(client):
    LOOP_MS = 50
    logging.info(f"CSV playback: {CSV_START} → {CSV_END} (start={PLAYHEAD_BASE}, speed {PLAYBACK_SPEED}x, loop={LOOP_DATASET})")

    last_log = time.time()
    acc_published = 0

    while True:
        try:
            n = publish_tick(client)
            acc_published += n

            now = time.time()
            if now - last_log >= 1.0:
                logging.info(f"Published {acc_published} msgs in {now-last_log:.1f}s "
                             f"(~{acc_published/max(1e-3, now-last_log):.0f} msg/s)")
                acc_published = 0
                last_log = now

            time.sleep(LOOP_MS/1000.0)
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error(f"Error in publish loop: {e}", exc_info=True)
            time.sleep(1)

# =========================
# Entry point
# =========================
def run():
    global SENSORS, EVENTS, CSV_START, CSV_END, PLAYHEAD_BASE, SENSOR_STATES, SENSOR_META, WALLCLOCK_T0

    logging.info("Loading CSV data...")
    SENSORS, EVENTS, CSV_START, CSV_END = load_data()

    logging.info(f"SENSORS loaded: {len(SENSORS)} "
                 f"(ppg={int((SENSORS['raw_kind']=='ppg').sum())}, "
                 f"acc={int((SENSORS['raw_kind']=='accelerometer').sum())}, "
                 f"ceda={int((SENSORS['raw_kind']=='ceda').sum())}, "
                 f"skin={int((SENSORS['raw_kind']=='skin_temp').sum())})")
    logging.info(f"EVENTS loaded : {len(EVENTS)} rows "
                 f"(first={EVENTS['start'].min()}, last={EVENTS['end'].max()})")
    logging.info(f"Users (sensors): {sorted(SENSORS['user_id'].unique().tolist())}")
    logging.info(f"Users (events) : {sorted(EVENTS['user_id'].unique().tolist())}")

    PLAYHEAD_BASE = compute_playhead_base()
    WALLCLOCK_T0 = time.time()

    SENSOR_STATES, SENSOR_META = build_states()
    logging.info(f"Built states for {len(SENSOR_STATES)} sensors")

    logging.info("Waiting for MQTT broker...")
    if not wait_for_mqtt_broker():
        logging.error("MQTT broker not available")
        return

    client = connect_mqtt_with_retry()
    if not client:
        logging.error("MQTT connection failed")
        return

    logging.info("MQTT connected. Publishing...")
    publish_loop(client)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,  # usa DEBUG per vedere ogni publish
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    time.sleep(30)
    print("CSV RAW Publisher starting… (Ctrl+C per uscire)")
    run()