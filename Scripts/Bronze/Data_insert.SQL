/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.


Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
Create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime,@end_time datetime,@batch_start datetime, @batch_end datetime
	set @batch_start=GETDATE()
	-------CRM FILES------------
	print'========================================='
	print 'Loading Data CRM into tables'
	print'========================================='

	set @start_time= GETDATE()
	truncate table bronze.crm_cust_info
	bulk insert bronze.crm_cust_info
	from 'E:\Projects\SQL Data warehouse\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time= GETDATE()
	print' >> Loading time crm_cust_info: ' + cast( datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print'----------------------------------'
	set @start_time= GETDATE()
	truncate table bronze.crm_prd_info
	bulk insert bronze.crm_prd_info
	from 'E:\Projects\SQL Data warehouse\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);

	set @end_time= GETDATE()
	print' >> Loading time crm_prd_info: ' + cast( datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print'----------------------------------'

	set @start_time= GETDATE()
	truncate table bronze.crm_sales_details
	bulk insert bronze.crm_sales_details
	from 'E:\Projects\SQL Data warehouse\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time= GETDATE()
	print' >> Loading time crm_sales_details: ' + cast( datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print'----------------------------------'

	----------------------------------
	/* ERP files */

	print'========================================='
	print 'Loading Data ERP into tables'
	print'========================================='

	set @start_time= GETDATE()
	truncate table bronze.erp_cust_az12
	bulk insert bronze.erp_cust_az12
	from 'E:\Projects\SQL Data warehouse\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time= GETDATE()
	print' >> Loading time erp_cust_az12: ' + cast( datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print'----------------------------------'

	set @start_time= GETDATE()
	truncate table bronze.erp_loc_a101
	bulk insert bronze.erp_loc_a101
	from 'E:\Projects\SQL Data warehouse\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time= GETDATE()
	print' >> Loading time erp_loc_a101: ' + cast( datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	print'----------------------------------'

	set @start_time= GETDATE()
	truncate table bronze.erp_px_cat_g1v2
	bulk insert bronze.erp_px_cat_g1v2
	from 'E:\Projects\SQL Data warehouse\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time= GETDATE()
	print' >> Loading time erp_px_cat_g1v2: ' + cast( datediff(second,@start_time,@end_time) as nvarchar) + 'seconds'
	

	set @batch_end=GETDATE()
	print'======================================'
	print' >> Bronze Batch Loading time : ' + cast( datediff(second,@batch_start,@batch_end) as nvarchar) + 'seconds'
	print'======================================'
end


-- exec bronze.load_bronze to execute the procedure---
