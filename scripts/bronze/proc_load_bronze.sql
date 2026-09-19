/*
=======================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=======================================================================

This stored procedure loads data into the 'bronze' schema. 
It performs the following actions:
	- Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data to bronze tables.
	- Accepts @source_path as a parameter, defaulting to local Windows
 	  path, so local usage is unaffected.
	- CI calls this with a Linux path for example:
      EXEC bronze.load_bronze @source_path = '/var/opt/mssql/datasets';

Usage (local):  EXEC bronze.load_bronze;
Usage (CI):     EXEC bronze.load_bronze @source_path = '/var/opt/mssql/datasets';
========================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
    @source_path NVARCHAR(500) = 'C:\sql\DA-course\sql-data-warehouse-project\datasets'
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	DECLARE @sql NVARCHAR(MAX);

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '========================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '========================================';

		PRINT '----------LOADING CRM TABLES------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		SET @sql = N'BULK INSERT bronze.crm_cust_info FROM ''' + @source_path + N'/source_crm/cust_info.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> crm_cust_info loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		SET @sql = N'BULK INSERT bronze.crm_prd_info FROM ''' + @source_path + N'/source_crm/prd_info.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> crm_prd_info loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		SET @sql = N'BULK INSERT bronze.crm_sales_details FROM ''' + @source_path + N'/source_crm/sales_details.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> crm_sales_details loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '----------LOADING ERP TABLES------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		SET @sql = N'BULK INSERT bronze.erp_cust_az12 FROM ''' + @source_path + N'/source_erp/cust_az12.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> erp_cust_az12 loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		SET @sql = N'BULK INSERT bronze.erp_loc_a101 FROM ''' + @source_path + N'/source_erp/loc_a101.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> erp_loc_a101 loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		SET @sql = N'BULK INSERT bronze.erp_px_cat_g1v2 FROM ''' + @source_path + N'/source_erp/px_cat_g1v2.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> erp_px_cat_g1v2 loaded in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end_time = GETDATE();
		PRINT '=================================';
		PRINT 'LOADING BRONZE LAYER IS COMPLETED';
		PRINT '- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=================================';
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR Message: ' + ERROR_MESSAGE();
		PRINT 'ERROR Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		THROW;
	END CATCH
END
