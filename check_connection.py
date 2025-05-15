import psycopg2
from params import *

def check_postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname= DB_NAME, 
            user= USER,
            password= PASSWORD,
            host= RDS_ENDPOINT,
            port= PORT
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        cur.close()
        conn.close()
        if result and result[0] == 1:
            print(f"Connection successful for {DB_NAME}!")
            return True
        else:
            print("Connection failed: Unexpected query result.")
            return False
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

check_postgres_connection()