import time
import json
import random
from datetime import datetime, timezone

def generate_heart_data(sensor_id="sensor_001"):
    return {
        "t": datetime.now(timezone.utc).isoformat(),
        "id": sensor_id,
        "hr": random.randint(60, 100)  # battito cardiaco realistico
    }
print(generate_heart_data(sensor_id="sensor_001"))

