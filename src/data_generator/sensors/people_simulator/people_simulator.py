from paho.mqtt import client as mqtt_client
import json
import time
import random
import logging
import socket
from datetime import datetime
import pytz
from utility import database_connection as db
import redis

r = redis.Redis(host='redis', port=6379, decode_responses=True, db = 1)

# MQTT configuration
broker = 'emqx1'  # Docker container name of EMQX in the same network
port = 1883  # MQTT TCP port (without TLS)
client_id = "people_simulator"
username = 'emqx'
password = 'public'
topic_prefix = "wearables"

# Reconnection strategy
FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60

# Startup wait strategy
STARTUP_MAX_RETRIES = 30  # Tentativi massimi all'avvio
STARTUP_RETRY_DELAY = 2  # Secondi tra un tentativo e l'altro

# Country to timezone mapping
COUNTRY_TIMEZONE_MAP = {
    'Italy': 'Europe/Rome',
    'United States': 'America/New_York',
    'United Kingdom': 'Europe/London',
    'Germany': 'Europe/Berlin',
    'France': 'Europe/Paris',
    'Spain': 'Europe/Madrid',
    'Japan': 'Asia/Tokyo',
    'China': 'Asia/Shanghai',
    'Australia': 'Australia/Sydney',
    'Canada': 'America/Toronto',
    'Brazil': 'America/Sao_Paulo',
    'India': 'Asia/Kolkata',
    'Russia': 'Europe/Moscow',
    'Mexico': 'America/Mexico_City',
    'South Korea': 'Asia/Seoul',
    'Netherlands': 'Europe/Amsterdam',
    'Switzerland': 'Europe/Zurich',
    'Sweden': 'Europe/Stockholm',
    'Norway': 'Europe/Oslo',
    'Denmark': 'Europe/Copenhagen',
    # Aggiungi altri paesi secondo necessità
}


# ---------------------------
# MQTT callbacks
# ---------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Connected to MQTT Broker successfully")
        print("Connected to MQTT Broker")
    else:
        logging.error(f"Failed to connect to MQTT Broker, return code {rc}")
        print(f"Failed to connect, return code {rc}")


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

    logging.critical("Reconnect failed after %d attempts. Exiting...", reconnect_count)


def on_publish(client, userdata, mid, reason_code=None, properties=None):
    """Callback for when a message is published - compatible with API v2"""
    if reason_code is not None and reason_code.value != 0:
        logging.warning(f"Message {mid} publish failed with reason code: {reason_code}")
    else:
        logging.debug(f"Message {mid} published successfully")


def wait_for_mqtt_broker():
    """Wait for MQTT broker to be ready with exponential backoff"""
    for attempt in range(1, STARTUP_MAX_RETRIES + 1):
        try:
            logging.info(f"Checking if MQTT broker is ready... (attempt {attempt}/{STARTUP_MAX_RETRIES})")

            # Test TCP connection to broker
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)  # 5 second timeout
            result = sock.connect_ex((broker, port))
            sock.close()

            if result == 0:
                logging.info(f"MQTT broker {broker}:{port} is ready!")
                return True
            else:
                logging.warning(f"MQTT broker not ready yet (connection failed)")

        except Exception as e:
            logging.warning(f"Error checking broker availability: {e}")

        if attempt < STARTUP_MAX_RETRIES:
            wait_time = min(STARTUP_RETRY_DELAY * attempt, 30)  # Max 30 seconds
            logging.info(f"Waiting {wait_time} seconds before next attempt...")
            time.sleep(wait_time)

    logging.error(f"MQTT broker {broker}:{port} is not available after {STARTUP_MAX_RETRIES} attempts")
    return False


def connect_mqtt_with_retry():
    """Create and connect MQTT client with retry logic"""
    for attempt in range(1, MAX_RECONNECT_COUNT + 1):
        try:
            logging.info(f"Attempting MQTT connection... (attempt {attempt}/{MAX_RECONNECT_COUNT})")

            client = mqtt_client.Client(
                client_id=client_id,
                callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2
            )
            client.username_pw_set(username, password)
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect

            # Start the loop before connecting
            client.loop_start()

            # Attempt connection
            client.connect(broker, port, keepalive=60)

            # Wait for connection to be established
            connection_timeout = 10
            start_time = time.time()

            while not client.is_connected() and (time.time() - start_time) < connection_timeout:
                time.sleep(0.1)

            if client.is_connected():
                logging.info("MQTT connection established successfully!")
                return client
            else:
                logging.warning(f"Connection attempt {attempt} timed out")
                client.loop_stop()
                client.disconnect()

        except Exception as e:
            logging.error(f"Connection attempt {attempt} failed: {e}")
            if 'client' in locals():
                try:
                    client.loop_stop()
                    client.disconnect()
                except:
                    pass

        if attempt < MAX_RECONNECT_COUNT:
            wait_time = min(FIRST_RECONNECT_DELAY * (RECONNECT_RATE ** (attempt - 1)), MAX_RECONNECT_DELAY)
            logging.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)

    logging.error(f"Failed to establish MQTT connection after {MAX_RECONNECT_COUNT} attempts")
    return None
    """Establish MQTT connection with proper error handling"""
    try:
        client = mqtt_client.Client(
            client_id=client_id,
            callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2
        )
        client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        # Rimuoviamo on_publish per evitare problemi di compatibilità
        # client.on_publish = on_publish

        logging.info(f"Connecting to MQTT broker at {broker}:{port}")
        # Non chiamiamo connect() qui, lo faremo dopo aver avviato il loop
        return client
    except Exception as e:
        logging.error(f"Failed to create MQTT client: {e}")
        raise


# ---------------------------
# Database helpers
# ---------------------------
def wait_user():
    pass

def get_user_data():
    """Retrieve user data including timezone information and sensor metrics"""
    try:
        conn = db.connection()
        cur = conn.cursor()

        # Get users with their country information
        cur.execute("""
                    SELECT up.user_id, up.country, up.name, up.surname
                    FROM users_profile up
                    WHERE up.country IS NOT NULL
                    """)
        users_data = cur.fetchall()

        if not users_data:
            logging.warning("No users found in database")
            return {}

        # Create user dictionary with timezone info
        user_info = {}
        for user_id, country, name, surname in users_data:
            timezone = COUNTRY_TIMEZONE_MAP.get(country, 'UTC')
            user_info[user_id] = {
                'country': country,
                'timezone': timezone,
                'name': name,
                'surname': surname,
                'sensors': []
            }

        # Get sensors for each user with their metric units
        user_ids = list(user_info.keys())
        if user_ids:
            placeholders = ','.join(['%s'] * len(user_ids))
            cur.execute(f"""
                SELECT stu.user_id, stu.sensor_id, stu.sensor_type_id, st.unit, st.name
                FROM sensor_to_user stu
                JOIN sensor_type st ON stu.sensor_type_id = st.sensor_type_id
                WHERE stu.user_id IN ({placeholders})
            """, user_ids)

            sensors_data = cur.fetchall()
            for user_id, sensor_id, sensor_type_id, unit, sensor_name in sensors_data:
                if user_id in user_info:
                    user_info[user_id]['sensors'].append((sensor_id, sensor_type_id, unit, sensor_name))

        conn.close()
        logging.info(f"Retrieved data for {len(user_info)} users")
        return user_info

    except Exception as e:
        logging.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.close()
        return {}


def get_user_timestamp(country):
    """Calculate timestamp based on user's country timezone"""

    return int(time.time() * 1000) # USA I MILLISECONDI

    """ Currently it doesn't work
    try:
        timezone_str = COUNTRY_TIMEZONE_MAP.get(country, 'UTC')
        user_timezone = pytz.timezone(timezone_str)
        local_time = datetime.now(user_timezone)
        return int(local_time.timestamp())
    except Exception as e:
        logging.warning(f"Error getting timezone for country {country}: {e}. Using UTC.")
        return int(time.time()) """


# ---------------------------
# Payload generation
# ---------------------------
def generate_sensor_value(sensor_type_id):
    """Generate realistic sensor values based on sensor type"""
    generators = {
        1: lambda: random.randint(1000, 5000),  # PPG raw ADC
        2: lambda: round(random.uniform(35.0, 37.5), 2),  # SkinTemp °C (more realistic range)
        3: lambda: [round(random.uniform(-2.0, 2.0), 3) for _ in range(3)],  # Accelerometer 3-axis (g)
        4: lambda: [round(random.uniform(-200.0, 200.0), 2) for _ in range(3)],  # Gyroscope 3-axis (deg/s)
        5: lambda: round(random.uniform(0, 2000), 1),  # Altimeter m
        6: lambda: round(random.uniform(950.0, 1050.0), 2),  # Barometer hPa (more realistic range)
        7: lambda: round(random.uniform(0.1, 20.0), 3),  # cEDA µS
    }

    generator = generators.get(sensor_type_id)
    if generator:
        return generator()
    else:
        logging.warning(f"Unknown sensor type ID: {sensor_type_id}")
        return random.random()


def generate_payload(sensor_id, sensor_type_id, metric, country):
    """Generate simplified sensor payload with user-specific timestamp"""
    timestamp = get_user_timestamp(country)


    payload = {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "metric": metric,
        "value": generate_sensor_value(sensor_type_id)
    }

    return payload


# ---------------------------
# Publish loop
# ---------------------------
def publish_sensor_data(client, user_info):
    """Publish sensor data for all users"""
    published_count = 0

    for user_id, info in user_info.items():

        country = info['country']
        name = info.get('name', 'Unknown')
        sensors = info['sensors']

        if not sensors:
            logging.debug(f"No sensors found for user {user_id} ({name})")
            continue

        for sensor_id, sensor_type_id, unit, sensor_name in sensors:
            if not r.exists(f"sensor:{sensor_id}"):
                logging.debug(f"Sensor {sensor_id} for user {user_id} not in Redis, skipping")
                continue
            try:
                payload = generate_payload(sensor_id, sensor_type_id, unit, country)

                topic = f"wearables/{sensor_name}"

                result = client.publish(topic, json.dumps(payload), qos=0)

                if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                    published_count += 1
                    logging.debug(f"Published to `{topic}`: {payload}")
                    print(f"Published to `{topic}` for {name} ({country}): "
                          f"metric={unit}, value={payload['value']}")
                else:
                    logging.error(f"Failed to publish to {topic}, error code: {result.rc}")

            except Exception as e:
                logging.error(f"Error publishing data for user {user_id}, sensor {sensor_id}: {e}")
                continue

    return published_count


def publish_loop(client):
    """Main publishing loop with periodic data refresh"""
    last_refresh = 0
    refresh_interval = 300  # Refresh user data every 5 minutes
    user_info = {}
    time.sleep(100)

    while True:
        try:
            current_time = time.time()

            # Refresh user data periodically
            if current_time - last_refresh > refresh_interval or not user_info:
                logging.info("Refreshing user data from database...")
                user_info = get_user_data()
                last_refresh = current_time

                if not user_info:
                    logging.warning("No user data available. Waiting before retry...")
                    time.sleep(30)
                    continue

            # Publish sensor data
            published_count = publish_sensor_data(client, user_info)
            logging.info(f"Published {published_count} sensor readings")

            # Wait before next iteration
            time.sleep(0.1)

        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt. Shutting down...")
            break
        except Exception as e:
            logging.error(f"Error in publish loop: {e}")
            time.sleep(10)  # Wait before retrying


# ---------------------------
# Entry point
# ---------------------------
def run():
    """Main application entry point"""
    try:
        logging.info("Starting MQTT sensor simulator...")

        # Step 1: Wait for MQTT broker to be ready
        if not wait_for_mqtt_broker():
            logging.error("MQTT broker is not available. Exiting...")
            return

        # Step 2: Connect to MQTT broker with retry
        client = connect_mqtt_with_retry()
        if not client:
            logging.error("Failed to connect to MQTT broker. Exiting...")
            return

        # Step 3: Start publishing loop
        logging.info("MQTT connection established. Starting publish loop...")
        publish_loop(client)

    except KeyboardInterrupt:
        logging.info("Application interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if 'client' in locals() and client:
            logging.info("Stopping MQTT client...")
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
        logging.info("Application shutdown complete")


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('sensor_simulator.log'),
            logging.StreamHandler()
        ]
    )

    print("Starting User Sensor Simulator...")
    print("Press Ctrl+C to stop")
    #run()