import pandas as pd
import numpy as np
import json

# === CONFIGURATION ===
xlsx_path = "simulazione_eventi_aprile_123.xlsx"

# === BOUNDED RANDOM WALK FUNCTION ===
def bounded_random_walk(mean, std_target, N, std_step=0.5, clip=None):
    walk = np.cumsum(np.random.normal(0, std_step, N))
    if np.std(walk) == 0:
        walk = np.full(N, mean)
    else:
        walk = walk - np.mean(walk)
        walk = walk / np.std(walk) * std_target
        walk += mean
    if clip:
        walk = np.clip(walk, *clip)
    return walk

# === LOAD AND CLEAN EXCEL FILE ===
df = pd.read_excel(xlsx_path, sheet_name="Foglio1", dtype=str)
df = df.iloc[:, 0].str.split(",", expand=True)
df.columns = [
    'user_id', 'start', 'end', 'activity', 'hr_mean', 'hr_std',
    'steps_lambda', 'hrv_mean', 'hrv_std', 'spo2_mean', 'spo2_std',
    'steps_std', 'skin_mean', 'skin_std'
]

# Type conversion
df["user_id"] = df["user_id"].astype(int)
df["start"] = pd.to_datetime(df["start"])
df["end"] = pd.to_datetime(df["end"])

# === PREPARE OUTPUT JSON ===
json_data = []

def append_series_to_json(series, sensor_id, variable_name):
    for ts, val in series.dropna().items():
        json_data.append({
            "sensor_id": int(sensor_id),
            "timestamp": ts.isoformat(),
            "value": float(round(val, 2)),
            "variable": variable_name
        })

# === PROCESS ALL USERS IN THE FILE ===
for user_id in df["user_id"].unique():
    df_user = df[df["user_id"] == user_id].copy()
    start_date = df_user["start"].min().normalize()
    end_date = df_user["end"].max().normalize() + pd.Timedelta(days=1)

    # Create time indices
    idx_5s = pd.date_range(start_date, end_date, freq="5s", inclusive="left")
    idx_30s = pd.date_range(start_date, end_date, freq="30s", inclusive="left")
    idx_2min = pd.date_range(start_date, end_date, freq="2min", inclusive="left")

    # Assign unique sensor IDs for the user
    base_sensor_id = (user_id - 1) * 5 + 1
    sensor_map = {
        "hr":    {"sensor_id": base_sensor_id,     "variable": "bpm"},
        "hrv":   {"sensor_id": base_sensor_id + 1, "variable": "hrv"},
        "spo2":  {"sensor_id": base_sensor_id + 2, "variable": "spo2"},
        "steps": {"sensor_id": base_sensor_id + 3, "variable": "steps"},
        "skin":  {"sensor_id": base_sensor_id + 4, "variable": "skin_temp"},
    }

    # Initialize empty time series
    hr = pd.Series(index=idx_5s, dtype=float)
    hrv = pd.Series(index=idx_30s, dtype=float)
    spo2 = pd.Series(index=idx_30s, dtype=float)
    steps = pd.Series(index=idx_5s, dtype=int)
    skin = pd.Series(index=idx_2min, dtype=float)

    # Generate synthetic data for each row of activity
    for _, row in df_user.iterrows():
        s = row["start"]
        e = row["end"]

        m5s = (idx_5s >= s) & (idx_5s < e)
        m30 = (idx_30s >= s) & (idx_30s < e)
        m2m = (idx_2min >= s) & (idx_2min < e)

        n5s, n30, n2m = m5s.sum(), m30.sum(), m2m.sum()

        if n5s > 0:
            hr.loc[m5s] = bounded_random_walk(float(row["hr_mean"]), float(row["hr_std"]), n5s, std_step=0.3)
            steps.loc[m5s] = np.random.poisson(float(row["steps_lambda"]), n5s)

        if n30 > 0:
            hrv.loc[m30] = bounded_random_walk(float(row["hrv_mean"]), float(row["hrv_std"]), n30, std_step=1.5)
            spo2.loc[m30] = bounded_random_walk(float(row["spo2_mean"]), 0.3, n30, std_step=0.1, clip=(95, 100))

        if n2m > 0:
            skin.loc[m2m] = bounded_random_walk(float(row["skin_mean"]), float(row["skin_std"]), n2m, std_step=0.05)

    # Add all sensor values to the JSON output
    append_series_to_json(hr, sensor_map["hr"]["sensor_id"], sensor_map["hr"]["variable"])
    append_series_to_json(hrv, sensor_map["hrv"]["sensor_id"], sensor_map["hrv"]["variable"])
    append_series_to_json(spo2, sensor_map["spo2"]["sensor_id"], sensor_map["spo2"]["variable"])
    append_series_to_json(steps, sensor_map["steps"]["sensor_id"], sensor_map["steps"]["variable"])
    append_series_to_json(skin, sensor_map["skin"]["sensor_id"], sensor_map["skin"]["variable"])

# === SORT JSON OUTPUT BY TIMESTAMP ===
json_data.sort(key=lambda x: x["timestamp"])

# === SAVE TO JSON FILE ===
with open("output_sensori_all_users.json", "w") as f:
    json.dump(json_data, f, indent=2)

# Preview the first 10 entries
print(json_data[:10])