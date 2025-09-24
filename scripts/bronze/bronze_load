/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure is responsible for loading data into the bronze schema from external CSV files.
    It carries out the following steps:
    - Clears existing data by truncating the bronze tables before each load.
    - Uses the COPY command to import data from CSV files into the bronze tables.

Parameters:
    None — this procedure does not take any input parameters and does not return any output values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/


create or replace procedure bronze.load_bronze ()
language plpgsql
as $$
declare
    rows_loaded BIGINT;
    start_time    timestamp;
    end_time      timestamp;
    elapsed       interval;
    elapsed_sec   bigint;
    total_start   timestamp;
    total_end     timestamp;
    total_elapsed interval;
    total_sec     bigint;
begin
    total_start := current_timestamp;

    RAISE NOTICE '--------------------------------%Loading Bronze layer', chr(10);
    RAISE NOTICE '--------------------------------';
    RAISE NOTICE '>> Loading CRM Tables';

    -------------------------------------------------------------------------
    -- bronze.source_crm_cust_info
    start_time := current_timestamp;

    TRUNCATE bronze.source_crm_cust_info;

    COPY bronze.source_crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    FROM '/var/lib/postgresql/import/cust_info.csv'
    WITH (FORMAT csv, HEADER true);

    SELECT COUNT(*) INTO rows_loaded FROM bronze.source_crm_cust_info;

    end_time := current_timestamp;
    elapsed  := end_time - start_time;
    elapsed_sec := EXTRACT(EPOCH FROM elapsed)::bigint;

    RAISE NOTICE 'Loaded % rows into bronze.source_crm_cust_info', rows_loaded;
    RAISE NOTICE '>> Load duration: % (≈ % seconds)', elapsed, elapsed_sec;

    -------------------------------------------------------------------------
    -- bronze.source_crm_prd_info
    start_time := current_timestamp;

    TRUNCATE bronze.source_crm_prd_info;

    COPY bronze.source_crm_prd_info (
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    FROM '/var/lib/postgresql/import/prd_info.csv'
    WITH (FORMAT csv, HEADER true);

    SELECT COUNT(*) INTO rows_loaded FROM bronze.source_crm_prd_info;

    end_time := current_timestamp;
    elapsed  := end_time - start_time;
    elapsed_sec := EXTRACT(EPOCH FROM elapsed)::bigint;

    RAISE NOTICE 'Loaded % rows into bronze.source_crm_prd_info', rows_loaded;
    RAISE NOTICE '>> Load duration: % (≈ % seconds)', elapsed, elapsed_sec;

    -------------------------------------------------------------------------
    -- bronze.source_crm_sales_details
    start_time := current_timestamp;

    TRUNCATE bronze.source_crm_sales_details;

    COPY bronze.source_crm_sales_details (
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
    FROM '/var/lib/postgresql/import/sales_details.csv'
    WITH (FORMAT csv, HEADER true);

    SELECT COUNT(*) INTO rows_loaded FROM bronze.source_crm_sales_details;

    end_time := current_timestamp;
    elapsed  := end_time - start_time;
    elapsed_sec := EXTRACT(EPOCH FROM elapsed)::bigint;

    RAISE NOTICE 'Loaded % rows into bronze.source_crm_sales_details', rows_loaded;
    RAISE NOTICE '>> Load duration: % (≈ % seconds)', elapsed, elapsed_sec;

    -------------------------------------------------------------------------
    RAISE NOTICE '--------------------------------';
    RAISE NOTICE '>> Loading ERP Tables';

    -------------------------------------------------------------------------
    -- bronze.source_erp_cust_az12
    start_time := current_timestamp;

    TRUNCATE bronze.source_erp_cust_az12;

    COPY bronze.source_erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    FROM '/var/lib/postgresql/import/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true);

    SELECT COUNT(*) INTO rows_loaded FROM bronze.source_erp_cust_az12;

    end_time := current_timestamp;
    elapsed  := end_time - start_time;
    elapsed_sec := EXTRACT(EPOCH FROM elapsed)::bigint;

    RAISE NOTICE 'Loaded % rows into bronze.source_erp_cust_az12', rows_loaded;
    RAISE NOTICE '>> Load duration: % (≈ % seconds)', elapsed, elapsed_sec;

    -------------------------------------------------------------------------
    -- bronze.source_erp_loc_a101
    start_time := current_timestamp;

    TRUNCATE bronze.source_erp_loc_a101;

    COPY bronze.source_erp_loc_a101 (
        cid,
        cntry
    )
    FROM '/var/lib/postgresql/import/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true);

    SELECT COUNT(*) INTO rows_loaded FROM bronze.source_erp_loc_a101;

    end_time := current_timestamp;
    elapsed  := end_time - start_time;
    elapsed_sec := EXTRACT(EPOCH FROM elapsed)::bigint;

    RAISE NOTICE 'Loaded % rows into bronze.source_erp_loc_a101', rows_loaded;
    RAISE NOTICE '>> Load duration: % (≈ % seconds)', elapsed, elapsed_sec;

    -------------------------------------------------------------------------
    -- bronze.source_erp_px_cat_g1v2
    start_time := current_timestamp;

    TRUNCATE bronze.source_erp_px_cat_g1v2;

    COPY bronze.source_erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    FROM '/var/lib/postgresql/import/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true);

    SELECT COUNT(*) INTO rows_loaded FROM bronze.source_erp_px_cat_g1v2;

    end_time := current_timestamp;
    elapsed  := end_time - start_time;
    elapsed_sec := EXTRACT(EPOCH FROM elapsed)::bigint;

    RAISE NOTICE 'Loaded % rows into bronze.source_erp_px_cat_g1v2', rows_loaded;
    RAISE NOTICE '>> Load duration: % (≈ % seconds)', elapsed, elapsed_sec;

    -------------------------------------------------------------------------
    -- Total duration
    total_end := current_timestamp;
    total_elapsed := total_end - total_start;
    total_sec := EXTRACT(EPOCH FROM total_elapsed)::bigint;

    RAISE NOTICE '--------------------------------';
    RAISE NOTICE '>> Total load duration: % (≈ % seconds)', total_elapsed, total_sec;

end;
$$;
