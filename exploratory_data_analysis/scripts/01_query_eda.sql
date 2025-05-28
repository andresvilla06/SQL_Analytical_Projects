	--02_Exploratory Data Analys--

/* 
Part I (Explore sechema tables and columns)
	1. Explore objects in the database (tables, ect.) | remember to choose the right DB
	2. Explore what are the columns in the datasets

Part II (Explore the dimensions)
	1. Identifying the unique values or categories for each dimension (using DISTINCT)
	
Part III (Date Exploration)
	1. Identify the boundaries in the dataset, earliest or latest date using MIN or MAX

Part IV (Explore Measures)
	1. Using the aggregate functions of sql
	
Part V (Magnitude) -> Measure + dimension
	1. Example, totalsales + by country
	
Part VI (Ranking)
	1.  ranking the measure by aggregating measure, ej. rank top contries by total sales
*/

---------------------------------------------------------------------------
--I
SELECT * FROM INFORMATION_SCHEMA.TABLES

SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'crm_cust_info'

---------------------------------------------------------------------------

--II
SELECT TOP(3) *
FROM gold.dim_customers;

SELECT DISTINCT
	country
FROM gold.dim_customers

SELECT DISTINCT category, subcategory, product_name
FROM gold.dim_products
ORDER BY 1,2,3
---------------------------------------------------------------------------

--III 
SELECT
	MIN(order_date) AS "earliest_date",
	MAX(order_date) AS "latest_date",
	DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) AS "year's_gap"
FROM gold.fact_sales;

---------------------------------------------------------------------------

--IV (creo que si vas a usar count en una misma query haciendo joins lo mejor seria una subquery en el SELECT, es mas creo que ni los JOIN son necesarios porque son puras agregaciones)

SELECT 
	SUM(fs.sales) AS "total_sales",
	AVG(fs.sales) AS "avg_price",
	SUM(fs.quantity) AS "total_quantity",
	COUNT(DISTINCT fs.order_number) AS "total_orders", --Se usa DISTINCT para no contar las ordenes duplicadas osea es que un customer pidio varias cosas en un misma orden
	(SELECT COUNT(product_name) FROM gold.dim_products) AS "total_products",
	(SELECT COUNT(customer_key) FROM gold.dim_customers) AS "total_customers",
	(SELECT COUNT(DISTINCT customer_key)) AS "total_placeorder_customers"
FROM gold.fact_sales AS fs

---

--Forma usando measure_name, measure_value usando UNION ALL
SELECT 'Total Sales' AS measure_name, SUM(sales) AS "measure_value" FROM gold.fact_sales
	UNION ALL
SELECT 'Average_Sales' AS measure_name, AVG(sales) AS "measure_value" FROM gold.fact_sales
	UNION ALL
SELECT'Total_Quantities' AS measure_name, SUM(quantity) AS "measure_value" FROM gold.fact_sales
	UNION ALL
SELECT'Total_Products' AS measure_name, COUNT(product_key) AS "measure_value" FROM gold.dim_products
	UNION ALL
SELECT'Total_Customers' AS measure_name, COUNT(customer_key) AS "measure_value" FROM gold.dim_customers
	UNION ALL
SELECT'Total_Customer2' AS measure_name, COUNT(customer_key) AS "measure_value" FROM gold.fact_sales


---------------------------------------------------------------------------

--V

SELECT 
	country,
	COUNT(customer_key) AS "total_customers"
FROM gold.dim_customers
GROUP BY country
ORDER BY 2 DESC
---
SELECT 
	gender,
	COUNT(customer_key) AS "total_customers"
FROM gold.dim_customers
GROUP BY gender
ORDER BY 2 DESC
---
SELECT 
	category,
	COUNT(product_key) AS "total_products"
FROM gold.dim_products
GROUP BY category
ORDER BY 2 DESC
---
SELECT 
	category,
	AVG(product_cost) AS "averagecost_per_category"
FROM gold.dim_products
GROUP BY category
ORDER BY 2 DESC
---
SELECT 
	dp.category,
	SUM(fs.sales) AS "total_per_category"
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS dp
	ON fs.product_key = dp.product_key
GROUP BY dp.category
ORDER BY 2 DESC
---
SELECT 
	dc.customer_key,
	CONCAT(dc.first_name,' ',dc.last_name) AS "full_name",
	SUM(fs.sales) AS "totalrevenue_per_customer"
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc
	ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_key, CONCAT(dc.first_name,' ',dc.last_name)
ORDER BY 3 DESC
---
SELECT 
	dc.country,
	SUM(fs.quantity) AS "totalitems_per_country"
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc
	ON fs.customer_key = dc.customer_key
GROUP BY dc.country
ORDER BY 2 DESC

---------------------------------------------------------------------------

--VI
SELECT 
	*
FROM (
	SELECT 
		dp.product_key,
		dp.product_name,
		SUM(fs.sales) AS "totalrevenue_per_product",
		RANK() OVER (ORDER BY SUM(fs.sales) DESC) AS "Ranking"
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_products dp
		ON fs.product_key = dp.product_key
	GROUP BY dp.product_key, dp.product_name
)sb 
WHERE Ranking <=5
---
SELECT 
	*
FROM (
	SELECT 
		dp.product_key,
		dp.product_name,
		SUM(fs.sales) AS "totalrevenue_per_product",
		RANK() OVER (ORDER BY SUM(fs.sales)) AS "Ranking"
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_products dp
		ON fs.product_key = dp.product_key
	GROUP BY dp.product_key, dp.product_name
)sb 
WHERE Ranking <=5
---
SELECT 
	*
FROM (
	SELECT 
		dc.customer_key,
		CONCAT(dc.first_name,' ',dc.last_name) AS "full_name",
		SUM(fs.sales) AS "totalrevenue_per_product",
		RANK() OVER (ORDER BY SUM(fs.sales) DESC) AS "ranking"
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_customers dc
		ON fs.customer_key = dc.customer_key
	GROUP BY dc.customer_key, CONCAT(dc.first_name,' ',dc.last_name)
)sb 
WHERE Ranking <=5
---
SELECT TOP 3 * 
FROM (
	SELECT 
		dc.customer_key,
		CONCAT(dc.first_name,' ',dc.last_name) AS "full_name",
		COUNT(DISTINCT fs.order_number) AS "totalorder_per_customer",
		RANK() OVER (ORDER BY SUM(fs.sales)) AS "ranking"
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_customers dc
		ON fs.customer_key = dc.customer_key
	GROUP BY dc.customer_key, CONCAT(dc.first_name,' ',dc.last_name)
)sb
WHERE totalorder_per_customer = 1
ORDER BY totalorder_per_customer
	
