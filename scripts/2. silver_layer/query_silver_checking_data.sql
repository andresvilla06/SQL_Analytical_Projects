/*	DATAWAREHOUSE Proyect 1

	--Silver Layer--

	1.After cleaning the bronze tables then finally can we aggregate to the silver layer tables
	2.Doing clening
	3. Doing the corresponding transformation (in the other query page)
	4. Do the same 3 previous steps for the remaining tables
*/

-------------------------------------------------------------------------------------

	--CHECKING DATA--

--Table 1: crm_cust_info

--2.1 Checking for nulls in the primary key (Replace bronze to silver to check later)
SELECT 
	COUNT(*) AS "nulls"
FROM bronze.crm_cust_info 
WHERE cst_id IS NULL;

-- 2.1.1 Checking duplicates in the primary key (null includes)
SELECT 
	cst_id,
	COUNT(*) AS "duplicate_id_nulls"
FROM bronze.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2.1.2 Check the blank spaces in the name and last name columns (the good result is not having any result, thats mean they are equal after the cleaning)
SELECT cst_lastname
FROM bronze.crm_cust_info 
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_firstname
FROM bronze.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname);

-- 2.1.3 Checking how many types are in the cardinal columns & consistency
-- Here we are saying we dont want abbrevations, just the full name using CASE
SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info;

---------------------------------

--Table 2: crm_prd_info

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

--2.1.4 Checking if the end date is smaller than the start date. 
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

--2.1.5 If the date dont make sense one solution is get rid off the end date and make a DATEDIFF with the start dates
--Here DATEADD works because, the date that LEAD bring me I gonna subtracted -1 Day 
SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt 
FROM bronze.crm_prd_info;

---------------------------------

--Table 3: crm_sales_details

SELECT 
sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

--2.1.6 Checking if are any key that are not in the silver cust_info
SELECT TOP (1000) *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
	SELECT cst_id FROM silver.crm_cust_info
);

--2.1.7 Negative and 0 numbers cant be cast to a date
SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;

--2.1.7 When casting a date int to date normal, check if the LEN date int is less than 8 and if the year is greater than the boundary
SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_order_dt) < 8 OR sls_order_dt > 20500101;

SELECT --Clean
	NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_ship_dt) < 8 OR sls_ship_dt > 20500101;

SELECT --Clean
	NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_due_dt) < 8 OR sls_due_dt > 20500101;

--2.1.8 Check that orderdate always need to be smaller than shiping date or Due Date
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

--2.1.9 Checking bussines rule: sales must be -> quantity * price, and are not allowed zero, negatives or null. (Baraa say that is case of bussinnes rules talk to the expert)

SELECT * 
FROM bronze.crm_sales_details
WHERE sls_quantity * sls_price != sls_sales;

SELECT DISTINCT 
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
WHERE sls_quantity * sls_price != sls_sales
	OR sls_price <= 0 OR sls_price IS NULL
	OR sls_quantity <= 0 OR sls_quantity IS NULL
	OR sls_sales <= 0 OR sls_sales IS NULL
ORDER BY sls_sales, sls_quantity, sls_price

 ---------------------------------

--Table 4: erp_cust_az12

SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS "cid",
	bdate,
	gen
FROM bronze.erp_cust_az12;

--2.1.9 checking if the date is to low or future dates
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS "cid",
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS "bdate",
	gen
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

SELECT DISTINCT
	gen 
FROM bronze.erp_cust_az12;

SELECT DISTINCT
	gen AS "gen_old",
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS "gen"
FROM bronze.erp_cust_az12


---------------------------------

--Table 5: erp_loc_a101

SELECT 
	cid AS "old_cid",
	REPLACE(cid, '-', '') AS "cid",
	cntry AS "old_cntry",
	CASE WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
		 WHEN UPPER(TRIM(cntry)) IN ('US', 'UNITED STATES', 'USA') THEN 'United States'
		 WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'n/a'
		 ELSE cntry
	END AS "cntry"
FROM bronze.erp_loc_a101


SELECT DISTINCT 
	cntry
FROM bronze.erp_loc_a101

----------------------------------------------------------

--Table 6: erp_px_cat_g1v2
SELECT * FROM silver.crm_prd_info
SELECT * FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2


