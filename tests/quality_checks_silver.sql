/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs data quality checks on the 'silver' layer to ensure 
    accuracy, consistency, and standardization. The checks include:
    - Null or duplicate primary keys.
    - Leading/trailing spaces in string fields.
    - Data standardization (categories, codes, enumerations).
    - Invalid date ranges or incorrect ordering.
    - Logical consistency across related fields.

Usage Notes:
    - Run these checks after populating Silver Layer tables.
    - Review and correct any issues returned by these queries.
===============================================================================
*/

-- ====================================================================
-- Check: silver.source_crm_cust_info
-- ====================================================================
-- Validate Primary Key (cst_id) → Expectation: No duplicates or NULLs
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.source_crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Detect leading/trailing spaces in keys → Expectation: No results
SELECT 
    cst_key 
FROM silver.source_crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Review distinct values for marital status → Expectation: Standardized set
SELECT DISTINCT 
    cst_marital_status 
FROM silver.source_crm_cust_info;

-- ====================================================================
-- Check: silver.source_crm_prd_info
-- ====================================================================
-- Validate Primary Key (prd_id) → Expectation: No duplicates or NULLs
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.source_crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Detect unwanted spaces in product names → Expectation: No results
SELECT 
    prd_nm 
FROM silver.source_crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULL or negative product cost → Expectation: No results
SELECT 
    prd_cost 
FROM silver.source_crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Review distinct product lines → Expectation: Standardized categories
SELECT DISTINCT 
    prd_line 
FROM silver.source_crm_prd_info;

-- Validate date order (start_date <= end_date) → Expectation: No results
SELECT 
    * 
FROM silver.source_crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Check: silver.source_crm_sales_details
-- ====================================================================
-- Validate order dates vs ship/due dates → Expectation: No violations
SELECT 
    * 
FROM silver.source_crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check consistency: sales = quantity * price → Expectation: No mismatches
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.source_crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Check: silver.source_erp_cust_az12
-- ====================================================================
-- Identify birthdates out of expected range → Expectation: Between 1924-01-01 and today
SELECT DISTINCT 
    bdate 
FROM silver.source_erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > CURRENT_DATE;

-- Review distinct gender codes → Expectation: Standardized values
SELECT DISTINCT 
    gen 
FROM silver.source_erp_cust_az12;

-- ====================================================================
-- Check: silver.source_erp_loc_a101
-- ====================================================================
-- Review distinct country codes → Expectation: Standardized values
SELECT DISTINCT 
    cntry 
FROM silver.source_erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Check: silver.source_erp_px_cat_g1v2
-- ====================================================================
-- Detect leading/trailing spaces in category fields → Expectation: No results
SELECT 
    * 
FROM silver.source_erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Review distinct maintenance categories → Expectation: Standardized values
SELECT DISTINCT 
    maintenance 
FROM silver.source_erp_px_cat_g1v2;

