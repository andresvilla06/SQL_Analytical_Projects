/*	DATAWAREHOUSE Proyect 1

	--Gold Layer | CHECKING--

	1. Checking all the data from silver and joining them
	2. Check the duplicates and do integration data

*/
-------------------------------------------------------------------------------------

	--CUSTOMER JOINING INFO--

	--TABLE 1
	/*	1.1 Create a primary key for the dimensions tables (surrogate key)
		1.2 Make the data integration for gender columns to one 	

	*/
SELECT 
	cst_id,
	COUNT(*) as duplicates
FROM (
	SELECT 
		cci.cst_id,
		cci.cst_key,
		cci.cst_firstname,
		cci.cst_lastname,
		cci.cst_marital_status,
		cci.cst_gender,
		eca.bdate,
		ela.cntry,
		eca.gen,
		cci.cst_create_date
	FROM silver.crm_cust_info AS cci
	LEFT JOIN silver.erp_cust_az12 AS eca
		ON cci.cst_key = eca.cid 
	LEFT JOIN silver.erp_loc_a101 AS ela
		ON cci.cst_key = ela.cid
)sub
GROUP BY cst_id
HAVING COUNT(*) > 1
;

--3.1 Checking and doing data integration (ask to the expert to know the answer)
SELECT DISTINCT
	cci.cst_gender,
	eca.gen,
	CASE WHEN cci.cst_gender != 'n/a' THEN cci.cst_gender
		 ELSE COALESCE(eca.gen, 'n/a') 
	END AS "new_gender"
FROM silver.crm_cust_info AS cci
LEFT JOIN silver.erp_cust_az12 AS eca
	ON cci.cst_key = eca.cid 
LEFT JOIN silver.erp_loc_a101 AS ela
	ON cci.cst_key = ela.cid
ORDER BY 1,2
;

----------------------------------

	--TABLE 2

	/*	1.1 Filter only for the present items, not the historical
		1.2 Checking for duplicates in prd_key	
		1.3 Create a row number primary key
	*/

SELECT 
	sub.prd_key,
	COUNT(*)
FROM(
	SELECT 
		cpi.prd_id,
		cpi.prd_cat_id,
		cpi.prd_key,
		cpi.prd_nm,
		cpi.prd_cost,
		cpi.prd_line,
		epx.cat,
		epx.subcat,
		epx.maintenance,
		cpi.prd_start_dt
	FROM silver.crm_prd_info AS cpi
	LEFT JOIN silver.erp_px_cat_g1v2 AS epx
		ON cpi.prd_cat_id = epx.id
	WHERE cpi.prd_end_dt IS NULL
)sub
GROUP BY sub.prd_key
HAVING COUNT(*) > 1
;

----------------------------------

	--TABLE 3

	/*	1.1 Detect if is a fact or dimension table
		1.2 Because is a fact table chance the old fk, to the new sugorrate key for better joining.
		1.3 Put friendly names to the columns
	*/

SELECT
	csd.sls_ord_num AS "order_number",
	gdc.customer_key,
	gdp.product_key,
	csd.sls_order_dt AS "order_date",
	csd.sls_ship_dt AS "shipping_date",
	csd.sls_due_dt AS "due_date",
	csd.sls_sales AS "sales",
	csd.sls_quantity AS "quantity",
	csd.sls_price AS "price"
FROM silver.crm_sales_details AS csd
LEFT JOIN gold.dim_customers AS gdc
	ON csd.sls_cust_id = gdc.customer_id
LEFT JOIN gold.dim_products AS gdp
	ON csd.sls_prd_key = gdp.product_number