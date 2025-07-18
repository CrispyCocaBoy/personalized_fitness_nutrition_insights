import pandas as pd
import numpy as np
import duckdb

# Connnessione aperta su postgresql
con = duckdb.connect()
con.execute("INSTALL postgres;")
con.execute("LOAD postgres;")
con.execute("""
    ATTACH 'host=users_profile_db user=admin password=admin dbname=users_profile' AS postgres_db (TYPE postgres, SCHEMA 'public');
""")


# Scrivere la query
df = con.execute("SELECT * FROM postgres_db.users").fetchdf()
print(df)

