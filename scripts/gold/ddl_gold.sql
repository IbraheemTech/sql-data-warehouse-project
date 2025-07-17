-------Gold Customer------
--------------------------

if OBJEECT ID('gold.dim_customers', 'V') IS NOT NULL 
  DROP VIEW gold.dim_customers;
GO
CREATE VIEW [gold].[dim_customers] AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  ---CRM IS THE MSTER FOR GENDER INFO
			ELSE COALESCE(ca.GEN, 'n/a')
	END AS gender,
	ci.cst_create_date AS create_date,
	ca.bdate AS birthdate,
	la.cntry AS country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON			ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		ci.cst_key = la.cid
GO

  ---------Gold Product-------
  ----------------------------

if OBJEECT ID('gold.dim_customers', 'V') IS NOT NULL 
  DROP VIEW gold.dim_customers;
GO

CREATE VIEW [gold].[dim_products] AS	
SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS prdouct_key,
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
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL ----FILETER OUT ALL HISTORICAL DATA
GO


  

-----------Fact sales-------
----------------------------

if OBJEECT ID('gold.fact_sales', 'V') IS NOT NULL 
  DROP VIEW gold.fact_sales;
GO

CREATE VIEW [gold].[dim_customers] AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  ---CRM IS THE MSTER FOR GENDER INFO
			ELSE COALESCE(ca.GEN, 'n/a')
	END AS gender,
	ci.cst_create_date AS create_date,
	ca.bdate AS birthdate,
	la.cntry AS country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON			ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		ci.cst_key = la.cid

GO
