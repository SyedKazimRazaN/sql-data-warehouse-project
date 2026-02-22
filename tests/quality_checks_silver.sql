/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-----------------------------------Quality check crm_prd_info TABLE---------------------------


--CHECKING NULL and DUPLICATES IN PRIMARY KEY
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) >1 OR prd_id IS NULL;


--CHECKING UNWANTED SPACES
SELECT
prd_nm
FROM silver.crm_prd_info
WHERE
prd_nm != TRIM(prd_nm);


--CHECKING NULLS or NEGATIVE NUMBERS
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;



--DATA CONSISTENCY & STANDARDIZATION
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


--CHECKINF INVALID DATE ORDERS
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


SELECT * 
FROM silver.crm_prd_info;



-----------------------------------Quality check crm_sales_details TABLE---------------------------

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE
	WHEN sls_order_dt <= 0 OR len(sls_order_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR(50)) AS DATE) 
	END AS sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM
bronze.crm_sales_details
WHERE TRIM(sls_ord_num) ! = sls_ord_num;

--CHECKING INVALID DATES, negative numbers and zeros
SELECT sls_order_dt
FROM bronze.crm_sales_details




SELECT NULLIF(sls_sales,(sls_sales <= 0))
from bronze.crm_sales_details
WHERE sls_sales <= 0;



--CHECKING INVALID DATE ORDERS
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


--CHECKING DATA CONSISTENCY B/W SALES< QUANTITY AND PRICE
SELECT DISTINCT *
FROM silver.crm_sales_details
WHERE sls_sales ! = sls_price * sls_quantity 
OR sls_sales <= 0 OR sls_price <= 0 OR sls_quantity <= 0 
OR sls_price IS NULL OR sls_sales IS NULL OR sls_price IS NULL




SELECT *,
CASE 
	WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price) 
	ELSE sls_sales
END AS sls_sales,
CASE 
	WHEN sls_price IS NULL OR sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

SELECT * FROM silver.crm_sales_details;





-----------------------------------Quality check erp_cust_az12 TABLE---------------------------

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;
 


SELECT
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END AS cid,
CASE 
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE
	WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12


-----------------------------------Quality check erp_loc_a101 TABLE---------------------------

SELECT * FROM bronze.erp_loc_a101;


--CHECKING DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT 
cntry AS old_cntry,
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE cntry
END AS cntry
from bronze.erp_loc_a101
ORDER BY cntry;




SELECT 
REPLACE(cid,'-','') AS cid,
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101;




-----------------------------------Quality check erp_px_cat_g1v2 TABLE---------------------------

SELECT * FROM bronze.erp_px_cat_g1v2;


--CHECKING FOR UNWANTED SPACING
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)OR maintenance != TRIM(maintenance);

SELECT DISTINCT cat from bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat from bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance from bronze.erp_px_cat_g1v2;


SELECT * FROM silver.erp_px_cat_g1v2;







