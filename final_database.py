import psycopg2
import os
import pandas as pd
from database import get_tables
import logging
from contextlib import closing
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def connect(database, user, password):
    # Connect to the default database to create the final_database if it doesn't exist
    default_conn = psycopg2.connect(database="northwind", user=user, password=password)
    default_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    default_cur = default_conn.cursor()

    # Check if the final_database exists
    default_cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{database}'")
    exists = default_cur.fetchone()

    # Create the final_database if it doesn't exist
    if not exists:
        default_cur.execute(f"CREATE DATABASE {database}")
        print(f"The '{database}' database has been created.")

    default_cur.close()
    default_conn.close()

    # Connect to the final_database
    return psycopg2.connect(database=database, user=user, password=password)


def create_table_and_load_data(df, table_name, cur):
    try:
        # Generate create table statement dynamically
        column_types = {
            'int64': 'INT',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64': 'TIMESTAMP',
            'timedelta[ns]': 'TIME'
        }
        cols_with_types = ', '.join([f'{col} {column_types[str(df[col].dtype)]}' for col in df.columns])
        create_table_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_with_types});"
        cur.execute(create_table_statement)

        # Add composite primary key for orders_details table
        if table_name == 'orders_details':
            cur.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (order_id, product_id);")

        logging.info(f"Table {table_name} created successfully")

        # Insert the data into the table
        for index, row in df.iterrows():
            placeholders = ', '.join(['%s' for _ in row])
            values = tuple(row.where(pd.notnull(row), None).tolist())  # Replace NaN with None
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
            cur.execute(insert_query, values)
        logging.info("Data inserted successfully")
    except Exception as e:
        logging.error(f"Failed to create table {table_name} dynamically: " + str(e))
        return []


def load_data_to_db(date):
    with closing(connect("final_database", "northwind_user", "thewindisblowing")) as conn, conn.cursor() as cur:
        tables = get_tables()
        for table in tables:
            if table == 'orders':
                postgres_files_path = os.path.join("data", "postgres", table, date, f"{table}.csv")
            else:
                postgres_files_path = os.path.join("data", "postgres", table, f"{table}.csv")
            df_postgres = pd.read_csv(postgres_files_path)
            create_table_and_load_data(df_postgres, table, cur)

        # Process orders_details.csv separately
        csv_file_path = os.path.join("data", "csv", "orders_details.csv")
        df_csv = pd.read_csv(csv_file_path)
        create_table_and_load_data(df_csv, "orders_details", cur)

        # Commit the changes to the database
        conn.commit()

        logging.info("Connected to the new database")


def final_query(date):
    with closing(connect("final_database", "northwind_user", "thewindisblowing")) as conn, conn.cursor() as cur:

        # Log orders table
        cur.execute("SELECT * FROM orders;")
        orders_rows = cur.fetchall()
        orders_column_names = [desc[0] for desc in cur.description]
        orders_df = pd.DataFrame(orders_rows, columns=orders_column_names)
        logging.info(f"Orders table:\n{orders_df}")

        # Log order_details table
        cur.execute("SELECT * FROM orders_details;")
        orders_details_rows = cur.fetchall()
        orders_details_column_names = [desc[0] for desc in cur.description]
        orders_details_df = pd.DataFrame(orders_details_rows, columns=orders_details_column_names)
        logging.info(f"Order details table:\n{orders_details_df}")

        # Final query
        cur.execute(
            """
            SELECT * 
            FROM orders 
            INNER JOIN orders_details 
            ON orders.order_id = orders_details.order_id
            """,
            (date,))
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]

        result_df = pd.DataFrame(rows, columns=column_names)
        result_file_path = os.path.join("data", "final_query_results", date, f"{date}_final_query_result.csv")
        os.makedirs(os.path.dirname(result_file_path), exist_ok=True)
        result_df.to_csv(result_file_path, index=False, header=True)

        logging.info("Final query run successfully")
