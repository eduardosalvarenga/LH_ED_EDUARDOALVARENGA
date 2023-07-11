import argparse
import logging
import os
import pandas as pd
from datetime import datetime
from database import get_tables, get_rows, get_column_names
from file_handling import save_to_csv, read_and_save_csv
from final_database import load_data_to_db, final_query

logging.basicConfig(filename='pipeline.log', level=logging.INFO)


def save_data_to_csv(date):
    # Convert date from 'DD/MM/YYYY' to 'YYYY-MM-DD'
    tables = get_tables()
    for table in tables:
        rows = get_rows(table, date)
        column_names = get_column_names(table)
        df = pd.DataFrame(rows, columns=column_names)
        save_to_csv(df, table, date)


def load_data_from_csv_to_db(date):
    load_data_to_db(date)
    final_query(date)


def main(input_date):
    if input_date is None:
        # Assign today's date in 'YYYY-MM-DD' format
        date = datetime.now().strftime('%Y-%m-%d')
    else:
        try:
            # Convert date from 'DD/MM/YYYY' to 'YYYY-MM-DD'
            date = datetime.strptime(input_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            logging.error("The date provided is not in the correct format. Please provide a date in the format "
                          "'DD/MM/YYYY'.")
            return

    # Step 1
    try:
        save_data_to_csv(date)
        read_and_save_csv()
        logging.info("Step 1 completed successfully.")
    except Exception as e:
        logging.exception("Error occurred in Step 1!")
        return

    # Check if files from Step 1 exist
    postgres_files_exist = all(
        os.path.exists(os.path.join("data", "postgres", table, date, f"{table}.csv")) if table == 'orders'
        else os.path.exists(os.path.join("data", "postgres", table, f"{table}.csv"))
        for table in get_tables()
    )
    csv_file_exists = os.path.exists(os.path.join("data", "csv", "orders_details.csv"))
    if not (postgres_files_exist and csv_file_exists):
        logging.error("Files from Step 1 are missing. Cannot proceed to Step 2.")
        print("Files from Step 1 are missing. Cannot proceed to Step 2.")
        return

    # Step 2
    try:
        load_data_from_csv_to_db(date)
        final_file = os.path.exists(os.path.join("data", "final_query_results", date, f"{date}_final_query_result.csv"))
        if final_file:
            logging.info("Step 2 completed successfully.")
        else:
            logging.info("There's no final file.")
    except Exception as e:
        logging.exception("Error occurred in Step 2!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Data Pipeline.')
    parser.add_argument('--date', type=str, help='Execution date in the format DD/MM/YYYY.')
    args = parser.parse_args()

    main(args.date)
