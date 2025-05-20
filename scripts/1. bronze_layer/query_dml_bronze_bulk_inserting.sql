/*	DATAWAREHOUSE Proyect 1

	--Bronze Layer--
	1. Here we are gonna insert the data in the columns created. 
	2. In the WITH clause is for specifying the rows, delimatetors, locked it.
	3. And before bulking the INSERT we need to truncate the TABLE to avoid re-inserting
	4. And then, creating a store procedure because is a often use query
	5. Then after creating the sp, create the TRY and CATCH and messages
	6. Track the ELT o extract duration using variables, getdate and datediff cast
	7. Doing the whole duration batch
*/
-------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE bronze.loadsp_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT 'Loading bronze layer csv';
		PRINT '==========================';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT '1. Loading CRM tables';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\avill\Desktop\coding\sql\ms_sql_server\datawarehouse_proyect 1\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds.'
		PRINT '-------------------------';
		-------------------------------------------------------------------------------------

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\avill\Desktop\coding\sql\ms_sql_server\datawarehouse_proyect 1\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds.'
		PRINT '-------------------------';
		-------------------------------------------------------------------------------------

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\avill\Desktop\coding\sql\ms_sql_server\datawarehouse_proyect 1\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds.'
		PRINT '-------------------------';
		--==================================================================================================
		PRINT '';
		PRINT '======================================';
		PRINT '';
		PRINT '2. Loading ERP tables';
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\avill\Desktop\coding\sql\ms_sql_server\datawarehouse_proyect 1\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds.'
		PRINT '-------------------------';
		-------------------------------------------------------------------------------------

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\avill\Desktop\coding\sql\ms_sql_server\datawarehouse_proyect 1\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds.'
		PRINT '-------------------------';

		-------------------------------------------------------------------------------------

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\avill\Desktop\coding\sql\ms_sql_server\datawarehouse_proyect 1\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds.'
		PRINT '-------------------------';
		
		SET @batch_end_time = GETDATE()
		PRINT ''
		PRINT '>>> Total load duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds.'
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED during loadings Bronze Layer'
		PRINT 'Error Message' + ERROR_MESSAGE()
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR)
	END CATCH	
END 
