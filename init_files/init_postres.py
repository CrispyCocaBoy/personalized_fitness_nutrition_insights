import psycopg2
from pathlib import Path

# Percorso del file SQL (relativo alla posizione dello script Python stesso)
sql_file = Path(__file__).parent.parent / "database/create_database.sql"

# Parametri di connessione
conn_params = {
    "host": "localhost",   # usa "postgres" se lo lanci da un container
    "port": 5432,
    "dbname": "user_device_db",
    "user": "admin",
    "password": "admin"
}

# Caricamento dello script SQL
with open(sql_file, "r") as f:
    sql_script = f.read()

def initialize_db():
    try:
        print("⏳ Connessione al database...")
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cur = conn.cursor()
        print("🚀 Esecuzione script SQL...")
        cur.execute(sql_script)
        cur.close()
        conn.close()
        print("✅ Inizializzazione completata.")
    except Exception as e:
        print(f"❌ Errore durante l'inizializzazione: {e}")

if __name__ == "__main__":
    initialize_db()

