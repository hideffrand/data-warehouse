import psycopg2
from psycopg2.extras import RealDictCursor

def get_db():
    conn = psycopg2.connect(
        host="localhost",
        database="retail_dw",
        user="postgres",
        password="root"
    )
    return conn
