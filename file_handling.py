import pandas as pd
import os
import requests
from io import StringIO
import logging


def save_to_csv(df, table, date):
    if table == 'orders':
        file_path = os.path.join("data", "postgres", table, date, f"{table}.csv")
    else:
        file_path = os.path.join("data", "postgres", table, f"{table}.csv")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False)
    logging.info(f"Saved {table} data to {file_path}")


def read_and_save_csv(csv_url='https://raw.githubusercontent.com/techindicium/code-challenge/main/data'
                              '/order_details.csv'):
    # Download CSV data
    response = requests.get(csv_url)
    response.raise_for_status()  # Raise exception if invalid response.

    # Read the CSV data into a pandas DataFrame
    df_csv = pd.read_csv(StringIO(response.text))

    # Save the CSV data
    csv_file_path = os.path.join("data", "csv", "orders_details.csv")
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
    df_csv.to_csv(csv_file_path, index=False)
    logging.info(f"Saved data to {csv_file_path}")
