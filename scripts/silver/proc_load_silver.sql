/*
=================================================================================
Name       : silver.load_silver
Purpose    : ETL procedure to load and transform data from bronze to silver layer.
             - Transforms CRM and ERP tables.
             - Applies business rules and standardization logic.
             - Tracks execution time and errors.
Target     : silver schema (for reporting and analytics)
Source     : bronze schema (raw/landing data)
=================================================================================
*/




CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY 
	SET @batch_start_time = GETDATE();
	PRINT '=================================================';
	PRINT 'LOADING SILVER LAYER';
	PRINT '=================================================';

	PRINT '-------------------------------------------------';
	PRINT 'LOADING CRM TABLES';
	PRINT '-------------------------------------------------';


	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING DATA INTO: silver.crm_cust_info';

	TRUNCATE TABLE silver.crm_cust_info;

	PRINT '>>INSERTING DATA INTO: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
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
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER (TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER ((TRIM(cst_marital_status))) = 'M' THEN 'Married'
		 ELSE 'N/A'
	END cst_marital_status,

	CASE WHEN UPPER (TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
		 WHEN UPPER ((TRIM(cst_gndr))) = 'M' THEN 'MALE'
		 ELSE 'N/A'
	END cst_gndr,
	cst_create_date
	FROM(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t WHERE flag_last  =1

	SET @end_time = GETDATE()
	PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';


	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING DATA INTO: silver.crm_prd_info';

	TRUNCATE TABLE silver.crm_prd_info;

	PRINT '>>INSERTING DATA INTO:silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING (prd_key, 1,5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,

	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'

	END AS prd_line,
	CAST (prd_start_dt AS  DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE ) AS prd_end_dt
	from bronze.crm_prd_info
	SET @end_time = GETDATE()
	PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';



	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING DATA INTO: silver.crm_sales_details';

	TRUNCATE TABLE silver.crm_sales_details;

	PRINT '>>INSERTING DATA INTO: silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details(
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
	CASE WHEN sls_order_dt = 0 OR len(sls_order_dt) !=8 THEN NULL 
		ELSE  CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE)
	END AS sls_order_dt,

	CASE WHEN sls_ship_dt = 0 OR len(sls_ship_dt) !=8 THEN NULL 
		ELSE  CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE)
	END AS sls_ship_dt,

	CASE WHEN sls_due_dt = 0 OR len(sls_due_dt) !=8 THEN NULL 
		ELSE  CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE)
	END AS sls_due_dt,

	CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,

	CASE WHEN sls_price IS NULL OR sls_price<=0 
		THEN sls_sales/NULLIF(sls_quantity, 0)
		ELSE sls_sales
	END AS sls_price
	from bronze.crm_sales_details
	SET @end_time = GETDATE()
	PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';


	------------* ERP *---------------

	SET @start_time = GETDATE();

	PRINT '>>TRUNCATING DATA INTO: silver.erp_cust_az12';

	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT '>>INSERTING DATA INTO:silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12(cid, bdate, GEN)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , len(cid))
		ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(Gen)) IN ('F', 'FEMALE') THEN 'FEMALE'
		WHEN UPPER(TRIM(Gen)) IN ('M', 'MALE') THEN 'MALE'
		ELSE 'n/a'
	END AS GEN
	from bronze.erp_cust_az12;
	SET @end_time = GETDATE()
	PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';



	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING DATA INTO: silver.erp_loc_a101';

	TRUNCATE TABLE silver.erp_loc_a101;

	PRINT '>>INSERTING DATA INTO:silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101(
	cid, 
	cntry
	)
	SELECT
	REPLACE (cid, '-','') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'UNITED STATES'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE()
	PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';



	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING DATA INTO: silver.erp_px_cat_g1v2';

	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	PRINT '>>INSERTING DATA INTO:silver.erp_px_cat_g1v2';

	INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
	)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE()
	PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';


	SET @batch_end_time = GETDATE();
	PRINT '============================================================================================================'
	PRINT 'TOTAL LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time)AS NVARCHAR)+ 'seconds';
	PRINT '============================================================================================================'

	END TRY
	BEGIN CATCH
			PRINT '====================================================';
			PRINT 'ERROR OCURRED DURING LOADING BRONZE LAYER';
			PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
			PRINT ' ERROR MESSAGE'+ CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '====================================================';

	END CATCH
	
END
