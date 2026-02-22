/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

---------------------------LOAD SCRIPTS WITH STORED PROCEDURE------------------------------------------

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
--TRACKING ETL DURATION--
	DECLARE @start_time DATETIME, @end_time DATETIME, @batchstart_time  DATETIME, @batchend_time  DATETIME ;
	BEGIN TRY
		SET @batchstart_time = GETDATE();
		PRINT '===============================================================';
		PRINT'LOADING SILVER LAYER';
		PRINT '===============================================================';


		PRINT '---------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES'
		PRINT '---------------------------------------------------------------';

	-----------------crm_cust_info TABLE-----------------------
	--CHECKING /CLEANING & INSERTING DATA AFTER TRANSFORMATIONS
	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> INSERTING DATA INTO: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_martial_status,
		cst_gender,
		cst_create_date
	)
	SELECT 
		cst_id,
		cst_key,
		--REMOVING UNWANTED SPACES
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		--DATA NORMALIZATION/STANDARDIZATION & HANDLING MISSING DATA (DATA CLEANSING)
		CASE WHEN UPPER(TRIM(cst_martial_status)) ='M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_martial_status)) ='S' THEN 'Single'
			 ELSE 'n/a' 
		END AS cst_martial_status,
		CASE WHEN UPPER(TRIM(cst_gender)) ='M' THEN 'Male'
			 WHEN UPPER(TRIM(cst_gender)) ='F' THEN 'Female'
			 ELSE 'n/a' 
		END AS cst_gender,
		cst_create_date
	FROM (
	--REMOVING DUPLICATES
		SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS ROW_RANK
		FROM bronze.crm_cust_info
		)t
	--DATA FILTERING
	WHERE ROW_RANK = 1 AND cst_id IS NOT NULL;
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>--------------------';







	-----------------crm_prd_info TABLE-----------------------
	--CHECKING /CLEANING & INSERTING DATA AFTER TRANSFORMATIONS
	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> INSERTING DATA INTO: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, ----DERIVED NEW COLUMN BY EXTRACTING CATEGORY ID 
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, ----DERIVED NEW COLUMN BY EXTRACTING PRODUCT KEY 
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost, --HANDLING MISSING INFO AND NULLs
		CASE UPPER(TRIM(prd_line))
			WHEN   'M' THEN 'Mountain'
			WHEN   'R' THEN 'Road'
			WHEN   'S' THEN 'Other Sales'
			WHEN   'T' THEN 'Touring'
			ELSE 'n/a' 
		END AS prd_line, --DATA NORMALIZATION/STANDARDIZATION & HANDLING MISSING DATA (DATA CLEANSING)
		CAST(prd_start_dt AS DATE), --DATA TYPECASTING / DATA TRANSFORMATION
		CAST(
			LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1
			AS DATE) 
			AS prd_end_dt -- DATA ENRICHMENT i.e, CALCULATING (END DATE) AS ONE DAY BEFORE NEXT START DATE!
	FROM bronze.crm_prd_info;
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>--------------------';









	-----------------crm_sales_details TABLE-----------------------
	--CHECKING /CLEANING & INSERTING DATA AFTER TRANSFORMATIONS
	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> INSERTING DATA INTO: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE --HANDLING MISSING DATA, INVALID DATA AND DATA TRANSFORMATION + DATA TYPECASTING
			WHEN sls_order_dt <= 0 OR len(sls_order_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR(50)) AS DATE) --	DATA TYPECASTING
			END AS sls_order_dt,
		CASE --HANDLING MISSING DATA, INVALID DATA AND DATA TRANSFORMATION + DATA TYPECASTING
			WHEN sls_ship_dt <= 0 OR len(sls_ship_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR(50)) AS DATE) 
			END AS sls_ship_dt,
		CASE --HANDLING MISSING DATA, INVALID DATA AND DATA TRANSFORMATION + DATA TYPECASTING
			WHEN sls_due_dt <= 0 OR len(sls_due_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR(50)) AS DATE) 
			END AS sls_due_dt,
		CASE --HANDLING MISSING DATA, INVALID DATA
			WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price) --RECALCULATING SALES IF ORIGINAL VALUE IS MISSING OR INCORRECT
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE --HANDLING MISSING DATA, INVALID DATA
			WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity,0) --DERIVING PRICE IF ORIGINAL VALUE IS INVALID
			ELSE sls_price
	END AS sls_price
	FROM
	bronze.crm_sales_details;
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>--------------------';







	PRINT '---------------------------------------------------------------';
	PRINT 'LOADING ERP TABLES'
	PRINT '---------------------------------------------------------------';

	-----------------erp_cust_az12 TABLE-----------------------
	--CHECKING /CLEANING & INSERTING DATA AFTER TRANSFORMATIONS
	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> INSERTING DATA INTO: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT
		CASE --HANDLING INVALID DATA
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE --HANDLING INVALID DATA
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE --DATA NORMALIZATION and HANDLING MISSING VALUES & NULLS
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>--------------------';










	-----------------erp_loc_a101 TABLE-----------------------
	--CHECKING /CLEANING & INSERTING DATA AFTER TRANSFORMATIONS
	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> INSERTING DATA INTO: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
	)
	SELECT 
		REPLACE(cid,'-','') AS cid, -- HANDLING INVALID DATA
		CASE --HANDLING DATA NORMALIZATION AND MISSING DATA OR NULLS
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE cntry
		END AS cntry
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>--------------------';




	-----------------erp_px_cat_g1v2 TABLE-----------------------
	--CHECKING /CLEANING & INSERTING DATA AFTER TRANSFORMATIONS
	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> INSERTING DATA INTO: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 ( 
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>>--------------------';


		SET @batchend_time = GETDATE();
		PRINT '=======================================================================';
		PRINT '>> LOADING SILVER LAYER IS COMPLETED';
		PRINT '>> DURATION FOR LOADING SILVER LAYER (WHOLE BATCH): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================================================================';
	END TRY
	-----------------ERROR HANDLING and DEBUGING---------------
	BEGIN CATCH
		PRINT '=======================================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (Error_Number() AS NVARCHAR);
		PRINT 'Error Message' + CAST (Error_STATE() AS NVARCHAR);
		PRINT '=======================================================================';
	END CATCH
END

























