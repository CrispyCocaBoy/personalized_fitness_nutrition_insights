import psycopg2

def connection():
    return psycopg2.connect(host="postgres", dbname="users_profile", user="admin", password="admin")


