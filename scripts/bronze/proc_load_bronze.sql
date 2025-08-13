/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY FROM` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze ()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '===============================================================';

    RAISE NOTICE '---------------------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---------------------------------------------------------------';

    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM '/Users/josiahveloz/Desktop/Data Analytics/Baraa-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (
        FORMAT csv,
        DELIMITER ',',
        HEADER TRUE
    );
    end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

    start_time := clock_timestamp(); 
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM '/Users/josiahveloz/Desktop/Data Analytics/Baraa-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (
        FORMAT csv,
        DELIMITER ',',
        HEADER TRUE
    );
    end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';
    
    start_time := clock_timestamp();    
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM '/Users/josiahveloz/Desktop/Data Analytics/Baraa-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (
        FORMAT csv,
        DELIMITER ',',
        HEADER TRUE
    );
    end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';
     
    RAISE NOTICE '---------------------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '---------------------------------------------------------------';

    start_time := clock_timestamp(); 
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM '/Users/josiahveloz/Desktop/Data Analytics/Baraa-data-warehouse-project/datasets/source_erp/cust_az12.csv'
    WITH (
        FORMAT csv,
        DELIMITER ',',
        HEADER TRUE
    );
    end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM '/Users/josiahveloz/Desktop/Data Analytics/Baraa-data-warehouse-project/datasets/source_erp/loc_a101.csv'
    WITH (
        FORMAT csv,
        DELIMITER ',',
        HEADER TRUE
    );
    end_time := clock_timestamp();
    RAISE NOTICE 'Load Duration: % milliseconds', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 2);
    RAISE NOTICE '-----------------------';

    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/josiahveloz/Desktop/Data Analytics/Baraa-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
    WITH (
        FORMAT csv,
        DELIMITER ',',
        HEADER TRUE
    );
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
