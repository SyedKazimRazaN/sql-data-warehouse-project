# pipeline/run_pipeline.py

"""
Portfolio-ready Data Warehouse pipeline.
Triggers Bronze → Silver → Gold layers automatically.
Includes logging, duration tracking, and simple data quality checks.
"""

import pyodbc
from datetime import datetime

# ------------------ Logging ------------------
def log(message, level="INFO"):
    """Structured logger with timestamp and log level"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {message}")

# ------------------ Execute Stored Procedure ------------------
def execute_stored_procedure(cursor, sp_name):
    """Executes a given stored procedure and logs duration"""
    log(f"Starting {sp_name}...", "INFO")
    start_time = datetime.now()
    cursor.execute(f"EXEC {sp_name}")
    cursor.connection.commit()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    log(f"Completed {sp_name} in {duration:.2f} seconds.", "INFO")
    return duration

# ------------------ Data Quality Check ------------------
def check_silver_quality(cursor):
    """Run simple data quality checks on Silver layer"""
    log("Running Silver layer data quality checks...", "INFO")
    
    # Example: Check for NULLs in primary key
    cursor.execute("SELECT COUNT(*) FROM silver.crm_cust_info WHERE cst_id IS NULL")
    null_count = cursor.fetchone()[0]
    log(f"NULL cst_id count in Silver: {null_count}", "INFO")
    
    # Example: Check for duplicates in primary key
    cursor.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT cst_id) 
        FROM silver.crm_cust_info
    """)
    dup_count = cursor.fetchone()[0]
    log(f"Duplicate cst_id count in Silver: {dup_count}", "INFO")
    
    log("Silver layer quality checks completed.", "INFO")

# ------------------ Main Pipeline ------------------
def main():
    pipeline_start = datetime.now()
    log("Starting full Data Warehouse pipeline...", "INFO")

    # SQL Server connection
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            r"SERVER=localhost\SQLEXPRESS;"
            "DATABASE=DataWarehouse;"
            "Trusted_Connection=yes;"
        )
        cursor = conn.cursor()
        
        # ---------------- Run Bronze ----------------
        execute_stored_procedure(cursor, "bronze.load_bronze")

        # ---------------- Run Silver ----------------
        execute_stored_procedure(cursor, "silver.load_silver")

        # ---------------- Data Quality Checks ----------------
        check_silver_quality(cursor)

        # ---------------- Gold Layer (Views) ----------------
        log("Gold layer is built using SQL views.", "INFO")
        # Optional: execute a stored procedure if you have one
        # execute_stored_procedure(cursor, "gold.build_views")

        pipeline_end = datetime.now()
        total_duration = (pipeline_end - pipeline_start).total_seconds()
        log(f"FULL PIPELINE COMPLETED in {total_duration:.2f} seconds", "INFO")

    except Exception as e:
        log(f"ERROR: {e}", "ERROR")

    finally:
        cursor.close()
        conn.close()

# ------------------ Run ------------------
if __name__ == "__main__":
    main()
