/*	DATAWAREHOUSE Proyect 1

	--Gold Layer | TRANSFORMATION--

	1. Make the needed transformations
	2. Rename the table names to a friendlier ones
	3. Decide if is a Dimension or Fact table a create a surogate key
	4. After the transformation create the view and dont forget to use the prefix

*/
-------------------------------------------------------------------------------------
	
	--TABLE 1

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cci.cst_id) AS "customer_key",
	cci.cst_id AS "customer_id",
	cci.cst_key AS "customer_number",
	cci.cst_firstname AS "first_name",
	cci.cst_lastname AS "last_name",
	ela.cntry AS "country",
	CASE WHEN cci.cst_gender != 'n/a' THEN cci.cst_gender
		 ELSE COALESCE(eca.gen, 'n/a') 
	END AS "gender",
	cci.cst_marital_status AS "marital_status",
	eca.bdate AS "birthday_date",
	cci.cst_create_date AS "customer_creation_date"
FROM silver.crm_cust_info AS cci
LEFT JOIN silver.erp_cust_az12 AS eca
	ON cci.cst_key = eca.cid 
LEFT JOIN silver.erp_loc_a101 AS ela
	ON cci.cst_key = ela.cid

--------------------------------------------------

	--TABLE 2

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cpi.prd_start_dt, cpi.prd_key) AS "product_key",
	cpi.prd_id AS "product_id",
	cpi.prd_key AS "product_number",
	cpi.prd_nm AS "product_name",
	cpi.prd_cat_id AS "product_category_id",
	epx.cat AS "category",
	epx.subcat AS "subcategory",
	cpi.prd_line AS "product_line",
	epx.maintenance,
	cpi.prd_cost AS "product_cost",
	cpi.prd_start_dt AS "product_start_date"
FROM silver.crm_prd_info AS cpi
LEFT JOIN silver.erp_px_cat_g1v2 AS epx
	ON cpi.prd_cat_id = epx.id
WHERE cpi.prd_end_dt IS NULL
;

----------------------------------

	--TABLE 3

CREATE VIEW gold.fact_sales AS
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
left JOIN gold.dim_products AS gdp
	ON csd.sls_prd_key = gdp.product_number
