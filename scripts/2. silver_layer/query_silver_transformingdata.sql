
	--Silver Layer--

--DOING TRANSFORMATION--
/*
3. Using rank function to get the latest id as 1, and using a subquery to see 
	the greater than 1 to then only select that are equal to 1. And using
	TRIM to get rid off the blank spaces, then using CASE because we decide
	to not use abbrevations instead using the full name, and ofcourse, to
	make sure in the future we use the UPPER and TRIM in that column.
	After doing all that, we INSERT this query to the silver.crm_cust_info
	so we are inserting clean data to silver custo_info

3.1 After doing the load to the silver schema, make the same cleaning checking
	to check is everything is ok and up to date. 
3.2 Do the previous steps for the remainings dirty tables
*/

--Table 1: crm_cust_info
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
WHERE "rank" = 1 AND cst_id IS NOT NULL --Data Filtering
-------------------------------------------------------------------------------------

--Table 2: crm_prd_info

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS prd_cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info