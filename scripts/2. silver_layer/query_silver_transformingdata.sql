
	--Silver Layer--
-----------------------------------------------------------------------------------------

	--DOING TRANSFORMATION--
/*
3. Using rank function to get the latest id as 1, and using a subquery to see 
	the greater than 1 to then only select that are equal to 1. And using
	TRIM to get rid off the blank spaces, then using CASE because we decide
	to not use abbrevations instead using the full name, and ofcourse, to
	make sure in the future we use the UPPER and TRIM in that column.
	After doing all that, we INSERT this query to the silver.crm_cust_info
	so we are inserting clean data to silver custo_info

3.1 After doing the load to the silver schema, make the
same cleaning checking
	to check is everything is ok and up to date. 
3.2 Do the previous steps for the remainings dirty tables

4. After cleaning and doing the transformations, create a Store Procedure for the script
*/

-----------------------------------------------------------------------------------------

--Table 1: crm_cust_info
CREATE OR ALTER PROCEDURE silver.loadsp_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT 'Truncating & Inserting the data';
		SET @batch_start_time = GETDATE ();
		SET @start_time = GETDATE();
		---
		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			"cst_firstname",
			"cst_lastname",
			"cst_marital_status",
			"cst_gender",
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS "cst_firstname", --Data Cleaning
			TRIM(cst_lastname) AS "cst_lastname",
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' --Data Standarization
				 ELSE 'n/a'
			END AS "cst_marital_status",
			CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male' --Data Standarization
				 ELSE 'n/a'
			END AS "cst_gender",
			cst_create_date
		FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS "rank" --Removing duplicates and Nulls
			FROM bronze.crm_cust_info
		)sub
		WHERE "rank" = 1 AND cst_id IS NOT NULL; --Data Filtering
		---
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------'

		----------------------------------------------------------

		--Table 2: crm_prd_info
		SET @start_time = GETDATE();
		---
		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info (
			prd_id,
			prd_cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS prd_cat_id, --Derived Columns
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, --Derived Columns
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost, --Null handeling
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'			--Data Standariztion
				 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				 ELSE 'n/a'
			END AS prd_line,
			prd_start_dt,
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt --Data enrichment
		FROM bronze.crm_prd_info;
		---
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------'
		
		----------------------------------------------------------

		--Table 3: crm_sales_details

		SET @start_time = GETDATE();
		---
		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details (
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
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) < 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) --We cant not cant int to date inmediately
			END AS "sls_order_dt",
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) < 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) --We cant not cant int to date inmediately
			END AS "sls_ship_dt",
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) < 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) --We cant not cant int to date inmediately
			END AS "sls_due_dt",
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END AS "sls_sales",
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				 ELSE sls_price
			END AS "sls_price"
		FROM bronze.crm_sales_details
		---
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------'

		----------------------------------------------------------

		--Table 4: erp_cust_az12

		SET @start_time = GETDATE();
		---
		TRUNCATE TABLE silver.erp_cust_az12;
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --Removing NAS
				 ELSE cid
			END AS "cid",
			CASE WHEN bdate > GETDATE() THEN NULL --Handeling future birthdates
				 ELSE bdate
			END AS "bdate",
			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female' --Data Standaratization
				 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				 ELSE 'n/a'
			END AS "gen"
		FROM bronze.erp_cust_az12
		---
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------'

		----------------------------------------------------------

		--Table 5: erp_loc_a101

		SET @start_time = GETDATE();
		---
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT 
			REPLACE(cid, '-', '') AS "cid",
			CASE WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany' --Data Standarization
				 WHEN UPPER(TRIM(cntry)) IN ('US', 'UNITED STATES', 'USA') THEN 'United States'
				 WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'n/a'
				 ELSE TRIM(cntry)
			END AS "cntry"
		FROM bronze.erp_loc_a101;
		---
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------'
		
		----------------------------------------------------------

		--Table 6: erp_px_cat_g1v2

		SET @start_time = GETDATE()
		---
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
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
		---
		SET @end_time = GETDATE()
		PRINT 'Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------'
		PRINT ''
		SET @batch_end_time = GETDATE()
		PRINT 'Total Batch Loading Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED during loadings Bronze Layer'
		PRINT 'Error Message' + ERROR_MESSAGE()
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR)
	END CATCH
END