	-- 03_ADVANCE BUSINESS ANALYTICS--

/* 
Part I (Change over time)
	1. See the sales by year or month, etc.

Part II (Cumulative Analysis)
	1. To see the incremental amount by year, months

Part III (Performance analysis)
	1. Like a year over year analysis

Part IV (Part to whole analysis)
	1.	To find out the propotion of a part relative to the whole allowing us to understand which category has the greattest impact on the business

Part V (Data Segmentation) 
	1. Its to convert a measure to a dimension to do a comparasion with other measure
	
Part VI (Reporting)
	1.
*/

---------------------------------------------------------------------------

--I

SELECT 
	YEAR(order_date) AS "year_date",
	MONTH(order_date) AS "month_date",
	SUM(sales) AS "Sales",
	COUNT(DISTINCT customer_key) AS "total_customers",
	SUM(quantity) AS "total_quantity"
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date) 
ORDER BY order_date

-- Diferente variante 

SELECT 
	DATETRUNC(MONTH, order_date) AS "order_date",
	SUM(sales) AS "Sales",
	COUNT(DISTINCT customer_key) AS "total_customers",
	SUM(quantity) AS "total_quantity"
FROM gold.fact_sales
WHERE order_date IS NOT NULL AND DATETRUNC(MONTH, order_date) LIKE '2012%'
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY order_date

---------------------------------------------------------------------------

--II

SELECT --running total que se reinicia cada vez que pasa un nuevo año
	*,
	SUM(sb.total_by_month) OVER (PARTITION BY YEAR(sb.months) ORDER BY sb.months) AS "running_total"
FROM ( 
	SELECT
		DATETRUNC(MONTH, order_date) AS "months",
		SUM(sales) AS "total_by_month"
	FROM gold.fact_sales
	WHERE DATETRUNC(MONTH, order_date) IS NOT NULL 
	GROUP BY DATETRUNC(MONTH, order_date)
) sb
---
SELECT --moving average of price
	*,
	AVG(sb.totalavg_by_year) OVER (ORDER BY sb.years) AS "running_total"
FROM ( 
	SELECT
		DATETRUNC(YEAR, order_date) AS "years",
		AVG(price) AS "totalavg_by_year"
	FROM gold.fact_sales
	WHERE DATETRUNC(YEAR, order_date) IS NOT NULL 
	GROUP BY DATETRUNC(YEAR, order_date)
) sb

---------------------------------------------------------------------------

--III year over year analysis
WITH yearly_product_sales AS (
	SELECT 
		DATETRUNC(YEAR, fs.order_date) AS "year",
		dp.product_name,
		SUM(fs.sales) AS "tsales_per_product"
	FROM gold.fact_sales AS fs
	LEFT JOIN gold.dim_products AS dp
		ON fs.product_key = dp.product_key
	WHERE DATETRUNC(YEAR, fs.order_date)  IS NOT NULL
	GROUP BY DATETRUNC(YEAR, fs.order_date), dp.product_name
)
, average_per_sales AS (
	SELECT 
		"year",
		product_name,
		tsales_per_product,
		AVG(tsales_per_product) OVER (PARTITION BY product_name) AS "avg_sales",
		tsales_per_product - AVG(tsales_per_product) OVER (PARTITION BY product_name) AS "gap_avg"
	FROM yearly_product_sales
)

SELECT 
	*,
	CASE WHEN gap_avg < 0 THEN 'Below '
		 WHEN gap_avg > 0 THEN 'Above'
		 ELSE 'Equal'
	END AS "flag",
	LAG (tsales_per_product) OVER (PARTITION BY product_name ORDER BY "year") AS "previous_year",
	tsales_per_product - LAG (tsales_per_product) OVER (PARTITION BY product_name ORDER BY "year") AS  "diff",
	CASE WHEN tsales_per_product - LAG (tsales_per_product) OVER (PARTITION BY product_name ORDER BY "year") > 0 THEN 'Increase'
		 WHEN tsales_per_product - LAG (tsales_per_product) OVER (PARTITION BY product_name ORDER BY "year") < 0 THEN 'Decrease'
		 ELSE 'No Change'
	END AS "py_change"
FROM average_per_sales
ORDER BY product_name, "year"
---------------------------------------------------------------------------

--IV
WITH total_per_category AS (
	SELECT 
		dp.category,
		SUM(fs.sales) AS "tsales_percategory"
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_products dp
		ON fs.product_key = dp.product_key
	GROUP BY dp.category
)

SELECT 
	category,
	tsales_percategory,
	SUM(tsales_percategory) OVER () "overall_sales",
	CONCAT(CAST(ROUND(CAST(tsales_percategory AS FLOAT)/ SUM(tsales_percategory) OVER () * 100, 2) AS VARCHAR), '%') AS "porcentaje"
FROM total_per_category
ORDER BY porcentaje DESC

---------------------------------------------------------------------------
--V
--Version 1
SELECT
	sb.product_name,
	sb.total_cost,
	sb.segment
FROM (
	SELECT 
		product_name,
		SUM(product_cost) AS "total_cost",
		NTILE(5) OVER (ORDER BY SUM(product_cost)) AS "segment"
	FROM gold.dim_products
	GROUP BY product_name
)sb
WHERE segment = 5;

--Version 2
WITH products_cost_range AS (
	SELECT 
		product_name,
		product_cost,
		CASE WHEN product_cost < 100 THEN 'Below 100'
			 WHEN product_cost BETWEEN 100 AND 500 THEN '100-500'
			 WHEN product_cost BETWEEN 500 AND 1000 THEN '500-1000'
			 ELSE 'Above 1000'
		END AS "cost_range"
	FROM gold.dim_products
)
SELECT 
	cost_range,
	COUNT(product_name) AS "total_products"
FROM products_cost_range
GROUP BY cost_range
ORDER BY total_products DESC;

---

WITH total_lifespan AS (
	SELECT 
		fs.customer_key,
		CONCAT(dc.first_name,' ', dc.last_name) AS "full_name",
		SUM(fs.sales) AS "total_customer_sales",
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS "lifespan"
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_customers dc
		ON fs.customer_key = dc.customer_key
	GROUP BY fs.customer_key, CONCAT(dc.first_name,' ', dc.last_name)
	
)
, total_tag_customer AS (
SELECT
	*,
	CASE WHEN total_customer_sales > 5000 AND lifespan >= 12 THEN 'VIP'
		 WHEN total_customer_sales <= 5000 AND lifespan >= 12 THEN 'Regular'
		 ELSE 'New'
	END AS "tag_customer"
FROM total_lifespan

)

SELECT tag_customer, COUNT(customer_key) AS "total_customers" 
FROM total_tag_customer
GROUP BY tag_customer
ORDER BY total_customers
---------------------------------------------------------------------------

--VI
CREATE VIEW gold.report_customers as 
WITH general_information AS ( --1st CTE
	SELECT 
		fs.order_number,
		fs.product_key,
		fs.order_date,
		fs.sales,
		fs.quantity,
		dc.customer_key,
		dc.customer_number,
		CONCAT(dc.first_name,' ', dc.last_name) AS "full_name",
		DATEDIFF(YEAR, birthday_date, GETDATE()) AS "years_old"
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_customers dc
		ON fs.customer_key = dc.customer_key
	WHERE order_date IS NOT NULL
)
, customer_agregations AS ( --2nd CTE
SELECT
    customer_key,
    customer_number,
    full_name,
    years_old,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales) AS total_customer_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT product_key) AS total_products,
    MAX(order_date) AS last_order_date,
    DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM general_information
GROUP BY
	 customer_key,
	 customer_number,
	 full_name,
	 years_old
)

SELECT 
	customer_key,
    customer_number,
    full_name,
    years_old,
	CASE WHEN years_old < 20 THEN 'Adolescene'
		 WHEN years_old BETWEEN 20 AND 29 THEN 'Young'
		 WHEN years_old BETWEEN 30 AND 59 THEN 'Mature'
		 WHEN years_old >= 60 THEN 'Advance Age'
		 ELSE 'No age aggregated'
	END AS "years_tag",
	CASE WHEN total_customer_sales > 5000 AND lifespan >= 12 THEN 'VIP'
		 WHEN total_customer_sales <= 5000 AND lifespan >= 12 THEN 'Regular'
		 ELSE 'New'
	END AS "tag_customer",
    total_orders,
    total_customer_sales,
    total_quantity,
    total_products,
    last_order_date,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS "recency",
    lifespan,
	NULLIF(total_customer_sales / total_orders, 0) AS "avg_order_value",
	CASE WHEN lifespan = 0 THEN total_customer_sales
		 ELSE total_customer_sales / lifespan
	END AS "avg_monthly_spend"
FROM customer_agregations 