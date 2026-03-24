# pipeline/run_pipeline.py

"""
Production-style Data Warehouse pipeline.
Includes:
✔ Bronze → Silver → Gold execution
✔ Step-level logging
✔ Pipeline run logging
✔ Error tracking
✔ Data quality checks
"""

import pyodbc
from datetime import datetime

# ------------------ Logging ------------------
def log(message, level="INFO"):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {message}")

# ------------------ Step Logger ------------------
def log_step(cursor, run_id, step_name, status, start, end, error=None):
    duration = (end - start).total_seconds()

    cursor.execute("""
        INSERT INTO monitoring.step_log
        (run_id, step_name, status, start_time, end_time, duration_seconds, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, run_id, step_name, status, start, end, duration, error)

    cursor.connection.commit()

# ------------------ Execute Stored Procedure ------------------
def execute_stored_procedure(cursor, sp_name):
    cursor.execute(f"EXEC {sp_name}")
    cursor.connection.commit()

# ------------------ Data Quality Check ------------------
def check_silver_quality(cursor):
    log("Running Silver layer data quality checks...", "INFO")

    cursor.execute("SELECT COUNT(*) FROM silver.crm_cust_info WHERE cst_id IS NULL")
    null_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT cst_id)
        FROM silver.crm_cust_info
    """)
    dup_count = cursor.fetchone()[0]

    log(f"NULL cst_id: {null_count}, Duplicates: {dup_count}", "INFO")

# ------------------ Main Pipeline ------------------
def main():
    pipeline_start = datetime.now()
    log("Starting full Data Warehouse pipeline...", "INFO")

    status = "Success"
    rows_loaded = 0

    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            r"SERVER=localhost\SQLEXPRESS;"
            "DATABASE=DataWarehouse;"
            "Trusted_Connection=yes;"
        )
        cursor = conn.cursor()

        # ---------------- Create Run ID ----------------
        cursor.execute("""
            INSERT INTO monitoring.pipeline_run_log (status, rows_loaded, duration_seconds)
            OUTPUT INSERTED.run_id
            VALUES ('Running', 0, 0)
        """)
        run_id = cursor.fetchone()[0]
        conn.commit()

        # ---------------- BRONZE ----------------
        start = datetime.now()
        try:
            log("Starting BRONZE...", "INFO")
            execute_stored_procedure(cursor, "bronze.load_bronze")
            end = datetime.now()
            log_step(cursor, run_id, "BRONZE", "SUCCESS", start, end)
        except Exception as e:
            end = datetime.now()
            log_step(cursor, run_id, "BRONZE", "FAILED", start, end, str(e))
            raise

        # ---------------- SILVER ----------------
        start = datetime.now()
        try:
            log("Starting SILVER...", "INFO")
            execute_stored_procedure(cursor, "silver.load_silver")
            end = datetime.now()
            log_step(cursor, run_id, "SILVER", "SUCCESS", start, end)
        except Exception as e:
            end = datetime.now()
            log_step(cursor, run_id, "SILVER", "FAILED", start, end, str(e))
            raise

        # ---------------- DATA QUALITY ----------------
        start = datetime.now()
        try:
            check_silver_quality(cursor)
            end = datetime.now()
            log_step(cursor, run_id, "DATA_QUALITY", "SUCCESS", start, end)
        except Exception as e:
            end = datetime.now()
            log_step(cursor, run_id, "DATA_QUALITY", "FAILED", start, end, str(e))
            raise

        # ---------------- GOLD ----------------
        start = datetime.now()
        try:
            log("Building GOLD views...", "INFO")
            end = datetime.now()
            log_step(cursor, run_id, "GOLD", "SUCCESS", start, end)
        except Exception as e:
            end = datetime.now()
            log_step(cursor, run_id, "GOLD", "FAILED", start, end, str(e))

        # ---------------- ROW COUNT ----------------
        cursor.execute("SELECT COUNT(*) FROM gold.fact_sales")
        rows_loaded = cursor.fetchone()[0]

        pipeline_end = datetime.now()
        total_duration = int((pipeline_end - pipeline_start).total_seconds())

        log(f"PIPELINE COMPLETED in {total_duration}s", "INFO")

    except Exception as e:
        status = "Failed"
        log(f"PIPELINE FAILED: {e}", "ERROR")
        total_duration = int((datetime.now() - pipeline_start).total_seconds())

    finally:
        # ---------------- UPDATE RUN LOG ----------------
        try:
            cursor.execute("""
                UPDATE monitoring.pipeline_run_log
                SET status = ?, rows_loaded = ?, duration_seconds = ?
                WHERE run_id = ?
            """, (status, rows_loaded, total_duration, run_id))

            conn.commit()
            log("Pipeline run logged successfully.", "INFO")

        except Exception as log_error:
            log(f"Logging failed: {log_error}", "ERROR")

        cursor.close()
        conn.close()

# ------------------ Run ------------------
if __name__ == "__main__":
    main()
