/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script runs quality checks to verify the integrity, consistency, and 
    accuracy of the Gold Layer. These checks ensure:
    - Surrogate keys in dimension tables are unique.
    - Referential integrity exists between fact and dimension tables.
    - Relationships in the data model are valid for analytics.

Usage Notes:
    - Investigate and resolve any issues returned by these checks.
===============================================================================
*/

-- ====================================================================
-- Check: gold.dim_customers
-- ====================================================================
-- Verify uniqueness of Customer Key in gold.dim_customers
-- Expected Result: No duplicate rows should be returned
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Check: gold.dim_products
-- ====================================================================
-- Verify uniqueness of Product Key in gold.dim_products
-- Expected Result: No duplicate rows should be returned
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Check: gold.fact_sales
-- ====================================================================
-- Validate referential integrity between fact_sales and its dimensions
-- Expected Result: No rows where customer_key or product_key are missing
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;
