/*

====================================================
STORED PROCEDURE: Load Bronze Layer (Source=>Bronze)
====================================================
SCRIPT PURPOSE:
This stored procedure load the data into the 'bronze' schema for external CSV files.
It performs following actions:
- Truncates the bronze table before loading.
- It also perform "Bulk Insert' loading data from CSV files to bronze tables.
USAGE:
EXEC bronze.laod_bronze;

*/



CREATE OR ALTER PROCEDURE bronze.load_bronze As
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '===============================================';
	PRINT ' LOADING BRONZE LAYER ';
	PRINT '===============================================';




	PRINT '-----------------------------------------------';
	PRINT 'LOADING CRM TABLES';
	PRINT '-----------------------------------------------';



	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT 'INSERTING DATA INTO: bronze.crm_cust_info '
	BULK INSERT bronze.crm_cust_info
	FROM 'D:\Ibraheem Data\My data engineering Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '-----------';

	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT 'INSERTING DATA INTO: bronze.crm_prd_info ';
	BULK INSERT bronze.crm_prd_info
	FROM 'D:\Ibraheem Data\My data engineering Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '-----------';

	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT 'INSERTING DATA INTO: bronze.crm_sales_details '
	BULK INSERT bronze.crm_sales_details
	FROM 'D:\Ibraheem Data\My data engineering Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	SET @end_time = GETDATE()
	PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '-----------';

	----ERP ------
	PRINT '-----------------------------------------------';
	PRINT 'LOADING ERP TABLES';
	PRINT '-----------------------------------------------';


	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT 'INSERTING DATA INTO: bronze.erp_cust_az12 ';
	BULK INSERT bronze.erp_cust_az12
	FROM 'D:\Ibraheem Data\My data engineering Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '-----------';


	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT 'INSERTING DATA INTO: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'D:\Ibraheem Data\My data engineering Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '-----------';

	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE:bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	PRINT 'INSERTING DATA INTO: bronze.erp_px_cat_g1v2 ';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'D:\Ibraheem Data\My data engineering Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	SET @end_time = GETDATE()
	PRINT '>> LOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '-----------';

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
