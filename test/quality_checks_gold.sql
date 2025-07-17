/*
===============================================================================
🎯 PURPOSE: Gold Layer View Creation - Data Warehouse
DESCRIPTION: These views apply normalization, surrogate keys, filtering,
             and standard transformations using data from the Silver Layer.

STANDARDS USED:
- ✅ Surrogate keys using ROW_NUMBER()
- ✅ LEFT JOINs to normalize and enrich data
- ✅ Coalescing and defaulting for missing values
- ✅ Naming: snake_case or camelCase depending on source
===============================================================================
*/
-- ============================================================================
-- 🔹 DIM_CUSTOMERS VIEW
-- PURPOSE: Stores cleansed and normalized customer demographic information
-- NORMALIZATION: Combines CRM + ERP sources
-- SURROGATE KEY: customer_key via ROW_NUMBER()
-- ============================================================================


CREATE VIEW gold.dim_customers AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,           -- Surrogate key
  ci.cst_id AS customer_id,                                         -- Natural key from CRM
  ci.cst_key AS customer_number,
  ci.cst_firstname AS first_name,
  ci.cst_lastname AS last_name,
  ci.cst_marital_status AS marital_status,
  CASE 
    WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr                      -- Prefer CRM gender
    ELSE COALESCE(ca.GEN, 'n/a')                                    -- Fallback to ERP
  END AS gender,
  ci.cst_create_date AS create_date,
  ca.bdate AS birthdate,
  la.cntry AS country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid            -- ERP customer extension
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;            -- ERP location info
GO

-- ============================================================================
-- 🔹 DIM_PRODUCTS VIEW
-- PURPOSE: Provides enriched and deduplicated product catalog
-- NORMALIZATION: Combines CRM product info with ERP category metadata
-- FILTER: Excludes historical products (prd_end_dt IS NULL)
-- SURROGATE KEY: product_key via ROW_NUMBER()
-- ============================================================================


CREATE VIEW gold.dim_products AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
  pn.prd_id AS product_id,
  pn.prd_key AS product_number,
  pn.prd_nm AS product_name,
  pn.cat_id AS category_id,
  pc.cat AS category,
  pc.subcat AS subcategory,
  pc.maintenance,
  pn.prd_cost AS cost,
  pn.prd_line AS product_line,
  pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Exclude outdated/historical products
GO


