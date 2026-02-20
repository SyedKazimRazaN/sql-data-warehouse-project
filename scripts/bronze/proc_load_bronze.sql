/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from local csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


---------------------------LOAD SCRIPTS WITH STORED PROCEDURE------------------------------------------

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
--TRACKING ETL DURATION--
	DECLARE @start_time DATETIME, @end_time DATETIME, @batchstart_time  DATETIME, @batchend_time  DATETIME ;
	BEGIN TRY
		SET @batchstart_time = GETDATE();
		PRINT '===============================================================';
		PRINT'LOADING BRONZE LAYER';
		PRINT '===============================================================';


		PRINT '---------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES'
		PRINT '---------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Courses\Data Engineer\SQL\Projects\SQL DATA WAREHOUSE (Data Engineering Shadow Project)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,				--skipping column names from file
			FIELDTERMINATOR = ',',		-- delimetters/file seperators
			TABLOCK						-- locking table during insert for performance
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------';



		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Courses\Data Engineer\SQL\Projects\SQL DATA WAREHOUSE (Data Engineering Shadow Project)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,				--skipping column names from file
			FIELDTERMINATOR = ',',		-- delimetters/file seperators
			TABLOCK						-- locking table during insert for performance
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------';
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details' ;
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Courses\Data Engineer\SQL\Projects\SQL DATA WAREHOUSE (Data Engineering Shadow Project)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,				--skipping column names from file
			FIELDTERMINATOR = ',',		-- delimetters/file seperators
			TABLOCK						-- locking table during insert for performance
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------';
		
		
		PRINT '---------------------------------------------------------------';
		PRINT 'LOADING ERP TABLES'
		PRINT '---------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Courses\Data Engineer\SQL\Projects\SQL DATA WAREHOUSE (Data Engineering Shadow Project)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,				--skipping column names from file
			FIELDTERMINATOR = ',',		-- delimetters/file seperators
			TABLOCK						-- locking table during insert for performance
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------';


		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';	
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Courses\Data Engineer\SQL\Projects\SQL DATA WAREHOUSE (Data Engineering Shadow Project)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,				--skipping column names from file
			FIELDTERMINATOR = ',',		-- delimetters/file seperators
			TABLOCK						-- locking table during insert for performance
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------';



		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Courses\Data Engineer\SQL\Projects\SQL DATA WAREHOUSE (Data Engineering Shadow Project)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,				--skipping column names from file
			FIELDTERMINATOR = ',',		-- delimetters/file seperators
			TABLOCK						-- locking table during insert for performance
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------';

		SET @batchend_time = GETDATE();
		PRINT '=======================================================================';
		PRINT '>> LOADING BRONZE LAYER IS COMPLETED';
		PRINT '>> DURATION FOR LOADING BRONZE LAYER (WHOLE BATCH): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================================================================';
	END TRY
	-----------------ERROR HANDLING and DEBUGING---------------
	BEGIN CATCH
		PRINT '=======================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (Error_Number() AS NVARCHAR);
		PRINT 'Error Message' + CAST (Error_STATE() AS NVARCHAR);
		PRINT '=======================================================================';
	END CATCH
END

