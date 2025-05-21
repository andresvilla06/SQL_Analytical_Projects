/*	DATAWAREHOUSE Proyect 1

	--Silver Layer--

	1.After cleaning the bronze tables then finally can we aggregate to the silver layer tables
	2.Doing clening
	3. Doing the corresponding transformation
	4. Do the same 3 previous steps for the remaining tables
*/

SELECT TOP (1000) *
FROM bronze.crm_cust_info;
SELECT TOP (1000) *
FROM bronze.crm_prd_info;
SELECT TOP (1000) *
FROM bronze.crm_sales_details;

SELECT TOP (1000) *
FROM bronze.erp_cust_az12;
SELECT TOP (1000) *
FROM bronze.erp_loc_a101;
SELECT TOP (1000) *
FROM bronze.erp_px_cat_g1v2;
-------------------------------------------------------------------------------------

	--CHECKING DATA--

--Table 1: crm_cust_info

--2.1 Checking for nulls in the primary key
--Replace bronze to silver to check later 
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
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_firstname
FROM bronze.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname)

-- 2.1.3 Checking how many types are in the cardinal columns & consistency
-- Here we are saying we dont want abbrevations, just the full name using CASE
SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info

---------------------------------

--Table 2: crm_prd_info

SELECT prd_key, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_key
HAVING COUNT(*) > 1 OR prd_key IS NULL

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)







---------------------------------