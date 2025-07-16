/*
=================================================================================
🧼 DATA CLEANING & TRANSFORMATION LOGIC — SILVER LAYER
Source: bronze schema      Target: silver schema
Purpose: Standardize, normalize, and clean raw data before use in analytics
=================================================================================
*/


-- =====================================================================
-- ========================= 🔵 CRM DATA ===============================
-- =====================================================================

-- 🔹 silver.crm_cust_info
-- Cleaning customer information
SELECT
    TRIM(cst_firstname) AS cst_firstname,              -- Remove leading/trailing spaces from first name
    TRIM(cst_lastname) AS cst_lastname,                -- Remove leading/trailing spaces from last name

    -- Normalize marital status
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END AS cst_marital_status,

    -- Normalize gender values
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
        ELSE 'N/A'
    END AS cst_gndr,

    -- Deduplication: Keep the latest record by cst_id
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info



-- 🔹 silver.crm_prd_info
-- Cleaning product information
SELECT
    -- Extract and normalize category ID from product key
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

    -- Extract clean product key
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,

    -- Replace NULL cost with 0
    ISNULL(prd_cost, 0) AS prd_cost,

    -- Normalize product line
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,

    -- Format dates correctly
    CAST(prd_start_dt AS DATE) AS prd_start_dt,

    -- Derive product end date from next start date
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info



-- 🔹 silver.crm_sales_details
-- Cleaning sales transaction records
SELECT
    -- Fix invalid order date (must be 8 digits, non-zero)
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,

    -- Same validation for ship and due dates
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,

    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,

    -- Recalculate sales if formula doesn’t match or value is NULL/invalid
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    -- Recalculate price if NULL or invalid
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details



-- =====================================================================
-- ========================= 🟢 ERP DATA ===============================
-- =====================================================================

-- 🔹 silver.erp_cust_az12
-- Cleaning ERP customer information
SELECT
    -- Remove 'NAS' prefix from customer ID
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,

    -- Remove future dates in birthdate field
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    -- Normalize gender values
    CASE 
        WHEN UPPER(TRIM(Gen)) IN ('F', 'FEMALE') THEN 'FEMALE'
        WHEN UPPER(TRIM(Gen)) IN ('M', 'MALE') THEN 'MALE'
        ELSE 'n/a'
    END AS GEN
FROM bronze.erp_cust_az12



-- 🔹 silver.erp_loc_a101
-- Cleaning ERP customer location data
SELECT
    -- Remove dashes from CID
    REPLACE(cid, '-', '') AS cid,

    -- Normalize country codes and handle missing/nulls
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'UNITED STATES'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101



-- 🔹 silver.erp_px_cat_g1v2
-- No transformations needed
-- Direct mapping from bronze to silver
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
