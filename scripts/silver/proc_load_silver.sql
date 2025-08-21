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
    CALL silver.load_silver ();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver ()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN
	batch_start_time := clock_timestamp();

    RAISE NOTICE '===============================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '===============================================================';

    RAISE NOTICE '---------------------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---------------------------------------------------------------';

    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	) 

	SELECT
	cst_id,
	cst_key,
	TRIM(BOTH ' ' FROM cst_firstname) AS cst_firstname,
	TRIM(BOTH ' ' FROM cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(BOTH ' ' FROM cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(BOTH ' ' FROM cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
		END AS cst_marital_status,
	CASE 
		WHEN UPPER(TRIM(BOTH ' ' FROM cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(BOTH ' ' FROM cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
		END AS cst_gndr,
	cst_create_date
	FROM (
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		) helper_table
	WHERE flag_last = 1;
	end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

	start_time := clock_timestamp(); 
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE 
		WHEN UPPER(TRIM(BOTH ' ' FROM prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(BOTH ' ' FROM prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(BOTH ' ' FROM prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(BOTH ' ' FROM prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info;
    end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

	start_time := clock_timestamp();    
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
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
	CASE 
		WHEN sls_order_dt <= 0 OR LENGTH(TO_CHAR(sls_order_dt, 'FM99999999')) != 8 THEN NULL
		ELSE TO_DATE(TO_CHAR(sls_order_dt, 'FM99999999'), 'YYYYMMDD')
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt <= 0 OR LENGTH(TO_CHAR(sls_ship_dt, 'FM99999999')) != 8 THEN NULL
		ELSE TO_DATE(TO_CHAR(sls_ship_dt, 'FM99999999'), 'YYYYMMDD')
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt <= 0 OR LENGTH(TO_CHAR(sls_due_dt, 'FM99999999')) != 8 THEN NULL
		ELSE TO_DATE(TO_CHAR(sls_due_dt, 'FM99999999'), 'YYYYMMDD')
	END AS sls_due_dt,
	CASE
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE
		WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;
	end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

	RAISE NOTICE '---------------------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '---------------------------------------------------------------';

    start_time := clock_timestamp(); 
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (
		cid, bdate, gen
	)
	SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4)
		ELSE cid
	END AS cid,
	CASE
		WHEN bdate > CURRENT_DATE THEN NULL
		ELSE bdate
	END AS bdate,
	CASE
		WHEN TRIM(BOTH ' ' FROM gen) IN ('M', 'MALE') THEN 'Male'
		WHEN TRIM(BOTH ' ' FROM gen) IN ('F', 'Female') THEN 'Female'
		ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;
	end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (
		cid, cntry
	)
	SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE
		WHEN cntry IS NULL OR cntry ~ '^[ ]+$' THEN 'n/a'
		WHEN TRIM(BOTH ' ' FROM cntry) IN ('USA', 'US') THEN 'United States'
		WHEN TRIM(BOTH ' ' FROM cntry) = 'DE' THEN 'Germany'
		ELSE TRIM(BOTH ' ' FROM cntry)
	END AS cntry
	FROM bronze.erp_loc_a101;
	end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

	start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

	batch_end_time := clock_timestamp();
    RAISE NOTICE '===============================================================';
    RAISE NOTICE'Loading Bronze Layer Completed';
    RAISE NOTICE '    - Total Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM batch_end_time - batch_start_time) * 1000, 2);
    RAISE NOTICE '===============================================================';
EXCEPTION
    WHEN others THEN 
        RAISE NOTICE '===============================================================';
        RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error message: %', SQLERRM;
        RAISE NOTICE '===============================================================';
END;
$$;
