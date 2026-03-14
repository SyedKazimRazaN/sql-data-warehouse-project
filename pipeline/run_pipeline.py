# pipeline/run_pipeline.py

"""
Run the full Data Warehouse pipeline.
This script triggers Bronze → Silver → Gold layers automatically.
"""

import pyodbc
from datetime import datetime

def log(message):
    """Simple logger to print timestamped messages"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def execute_stored_procedure(cursor, sp_name):
    """Executes a given stored procedure and logs duration"""
    log(f"Starting {sp_name}...")
    start_time = datetime.now()
    cursor.execute(f"EXEC {sp_name}")
    cursor.connection.commit()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    log(f"Completed {sp_name} in {duration:.2f} seconds.\n")

def main():
    # SQL Server connection
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\SQLEXPRESS;"
        "DATABASE=DataWarehouse;"
        "Trusted_Connection=yes;"
    )
    cursor = conn.cursor()

    try:
        # Run each layer sequentially
        execute_stored_procedure(cursor, "bronze.load_bronze")
        execute_stored_procedure(cursor, "silver.load_silver")
        log("Gold layer is built using SQL views.")

        log("Data Warehouse pipeline completed successfully.")

    except Exception as e:
        log(f"ERROR: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
