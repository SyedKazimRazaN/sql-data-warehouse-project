USE DataWarehouse;
GO

-- Create schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'monitoring')
BEGIN
    EXEC('CREATE SCHEMA monitoring');
END
GO

-- ===============================
-- PIPELINE RUN LOG (existing but safe recreate)
-- ===============================
IF OBJECT_ID('monitoring.pipeline_run_log', 'U') IS NOT NULL
    DROP TABLE monitoring.pipeline_run_log;
GO

CREATE TABLE monitoring.pipeline_run_log (
    run_id INT IDENTITY(1,1) PRIMARY KEY,
    run_timestamp DATETIME DEFAULT GETDATE(),
    status VARCHAR(20),
    rows_loaded INT,
    duration_seconds INT
);
GO

-- ===============================
-- STEP LEVEL LOG (NEW)
-- ===============================
IF OBJECT_ID('monitoring.step_log', 'U') IS NOT NULL
    DROP TABLE monitoring.step_log;
GO

CREATE TABLE monitoring.step_log (
    step_id INT IDENTITY(1,1) PRIMARY KEY,
    run_id INT,
    step_name VARCHAR(50),
    status VARCHAR(20),
    start_time DATETIME,
    end_time DATETIME,
    duration_seconds FLOAT,
    error_message VARCHAR(500)
);
GO
