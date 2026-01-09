/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as 
begin 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	begin try
	SET @batch_start_time = GETDATE();
	print'========================================================';
	print'Loading Bronze Layer';
	print'========================================================';

	print'--------------------------------------------------------';
	print'Loading crm Tables';
	print'--------------------------------------------------------';

	SET @start_time = GETDATE();
	print '>>Truncating table:bronze.crm_cust_info'
	Truncate table bronze.crm_cust_info;
	print '>> inserting data into :bronze.crm_cust_info'
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\SQL2022\SQL course\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'




	SET @start_time = GETDATE();
	print '>>Truncating table:bronze.crm_prd_info'
	Truncate table bronze.crm_prd_info;
	print '>> inserting data into :bronze.crm_prd_info'
	bulk insert bronze.crm_prd_info 
	from 'C:\SQL2022\SQL course\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';




	SET @end_time = GETDATE();
	print '>>Truncating table:bronze.crm_sales_details'
	Truncate table bronze.crm_sales_details;
	print '>> inserting data into :bronze.crm_sales_details'
	bulk insert bronze.crm_sales_details
	from 'C:\SQL2022\SQL course\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	with(
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';






	print'--------------------------------------------------------';
	print'Loading erp Tables';
	print'--------------------------------------------------------';

	SET @start_time = GETDATE();
	print '>>Truncating table:Bronze.erp_cust_az12'
	Truncate table Bronze.erp_cust_az12;
	print '>> inserting data into :Bronze.erp_cust_az12'
	bulk insert Bronze.erp_cust_az12
	from 'C:\SQL2022\SQL course\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
	with(
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


	SET @start_time = GETDATE();
	print '>>Truncating table:bronze.erp_loc_a101'
	Truncate table bronze.erp_loc_a101;
	print '>> inserting data into :bronze.erp_loc_a101'
	bulk insert bronze.erp_loc_a101
	from 'C:\SQL2022\SQL course\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
	with(firstrow = 2,
		fieldterminator=',',
		tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';




	SET @start_time = GETDATE();
	print '>>Truncating table:bronze.erp_px_g1v2'
	Truncate table bronze.erp_px_g1v2;
	print '>> inserting data into :bronze.erp_px_g1v2'
	bulk insert bronze.erp_px_g1v2
	from 'C:\SQL2022\SQL course\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
	with(firstrow = 2,
		fieldterminator=',',
		tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';






	end try
	begin catch
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
end

exec bronze.load_bronze
