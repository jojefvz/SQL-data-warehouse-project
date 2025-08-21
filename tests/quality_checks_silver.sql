-- bronze.crm_cust_info table --
-- finding IDs with duplicates or null IDs -- 
SELECT 
*,
COUNT(*)
FROM bronze.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- finding the latest of the existing duplicate or null IDs --
WITH helper_table1 AS (
    SELECT 
    cst_id
    FROM bronze.crm_cust_info 
    GROUP BY cst_id 
    HAVING COUNT(*) > 1 OR cst_id IS NULL
), helper_table2 AS (
    SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
    FROM bronze.crm_cust_info
    WHERE cst_id IN (SELECT * FROM helper_table1)
)

SELECT
*
FROM helper_table2
WHERE flag_last = 1;

-- checking for unwanted spaces --

SELECT
cst_firstname,
cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(BOTH ' ' FROM cst_firstname);

-- ---------------------------- --

-- bronze.crm_sales_details --
-- ALL DATA CHECKS --

-- FIRST COLUMN --
SELECT
sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(BOTH ' ' FROM sls_ord_num)


-- SECOND COLUMN --
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_key)

-- THIRD COLUMN --
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- FOURTH COLUMN --
SELECT
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
    OR sls_order_dt IS NULL
    OR LENGTH(TO_CHAR(sls_order_dt, 'FM99999999')) != 8
    OR sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

-- FIFTH COLUMN --
SELECT
sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
    OR sls_ship_dt IS NULL
    OR LENGTH(TO_CHAR(sls_ship_dt, 'FM99999999')) != 8
    OR sls_ship_dt > sls_due_dt;

-- SIXTH COLUMN --
SELECT
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR sls_due_dt IS NULL
OR LENGTH(TO_CHAR(sls_due_dt, 'FM99999999')) != 8;

-- SEVENTH COLUMN --
SELECT
sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL
OR sls_sales <= 0
OR sls_sales != sls_quantity * ABS(sls_price);

-- EIGTH COLUMN --
SELECT
sls_quantity
FROM bronze.crm_sales_details
WHERE sls_quantity IS NULL
OR sls_quantity <= 0;

-- NINTH COLUMN --
SELECT
sls_price
FROM bronze.crm_sales_details
WHERE sls_price IS NULL
OR sls_price <= 0;

-- ------------ END OF CHECKS ON bronze.crm_sales_details ------------ --

-- bronze.erp_cust_az12 --
-- checks for cid column --

SELECT
COUNT(*)
FROM bronze.erp_cust_az12;

SELECT
*
FROM bronze.erp_cust_az12
WHERE SUBSTR(cid, 4) NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- checks for bdate column --

SELECT
*
FROM silver.erp_cust_az12
WHERE bdate IS NULL OR bdate > CURRENT_DATE;

-- checks for gender column --

SELECT DISTINCT
gen
FROM bronze.erp_cust_az12;

-- ------------ END OF CHECKS ON bronze.erp_cust_az12 ------------ --

-- bronze.erp_loc_a101 -- 
-- checks on cid column --
SELECT
*
FROM bronze.erp_loc_a101
WHERE cid IS NULL
OR cid != TRIM(BOTH ' ' FROM cid);

SELECT
*
FROM bronze.erp_loc_a101
WHERE cid LIKE '%-%';

SELECT
*
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cust_key FROM silver.crm_cust_info);

-- checks on cntry column --
SELECT
*
FROM bronze.erp_loc_a101
WHERE cntry IS NULL;

SELECT
*
FROM bronze.erp_loc_a101
WHERE cntry != TRIM(BOTH ' ' FROM cntry);

SELECT DISTINCT
cntry
FROM bronze.erp_loc_a101;

-- ------------ END OF CHECKS ON bronze.erp_loc_a101 ------------ --

-- bronze.erp_px_cat_g1v2 --
-- checks on id column -- 
-- nothing to check --

-- checks on cat column --
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2;

-- checks on subcat column --
SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2;

SELECT
*
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(BOTH ' ' FROM subcat);

-- checks on maintenance column --
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT
*
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(BOTH ' ' FROM maintenance);

INSERT INTO silver.erp_px_cat_g1v2 --
