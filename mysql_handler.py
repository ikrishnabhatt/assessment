import pandas as pd
from sqlalchemy import create_engine
import pymysql

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'BACKEND'
}

engine = create_engine("mysql+pymysql://root:root@localhost/BACKEND")

def get_connection():
    return pymysql.connect(**db_config, cursorclass=pymysql.cursors.DictCursor)

def get_all_customers():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM customers")
            result = cursor.fetchall()
        return result
    finally:
        conn.close()

def get_customer_transactions(customer_id):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM transactions WHERE customer_id = %s", (customer_id,))
            result = cursor.fetchall()
        return result
    finally:
        conn.close()

def insert_transaction(customer_id, amount):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO transactions (customer_id, amount) VALUES (%s, %s)",
                (customer_id, amount)
            )
            conn.commit()
    finally:
        conn.close()

def customer_exists(customer_id):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM customers WHERE id = %s", (customer_id,))
            result = cursor.fetchone()
            return result['count'] > 0
    finally:
        conn.close()


def get_all_transactions_df():
    df = pd.read_sql("SELECT * FROM transactions", engine)
    return df
