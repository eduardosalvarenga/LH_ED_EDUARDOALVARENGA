import psycopg2
import logging

logging.basicConfig(filename='pipeline.log', level=logging.INFO)


def get_tables():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing"
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        table_names = [row[0] for row in cur.fetchall()]

        cur.close()
        conn.close()
        logging.info(f"Retrieved table names: {table_names}")
        return table_names

    except Exception as e:
        logging.error("Failed to get table names")
        return []


def get_rows(table, date):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing"
        )
        cur = conn.cursor()
        if table == 'orders':
            cur.execute(f"SELECT * FROM {table} WHERE order_date = %s", (date,))
        else:
            cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()

        cur.close()
        conn.close()
        logging.info("Retrieved rows")
        return rows

    except Exception as e:
        logging.error("Failed to get rows")
        return []


def get_column_names(table):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="northwind",
            user="northwind_user",
            password="thewindisblowing"
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table,))
        column_names = [row[0] for row in cur.fetchall()]

        cur.close()
        conn.close()
        logging.info(f"Retrieved column names {column_names} for table {table}")
        return column_names

    except Exception as e:
        logging.error("Failed to get columns names")
        return []
