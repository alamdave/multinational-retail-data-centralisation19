/* The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.

Find out the total and percentage of sales coming from each of the different store types.

The query should return: */

SELECT
    s.store_type,
	ROUND(SUM(o.product_quantity * p.product_price)::numeric,2) AS total_sales,
	ROUND(100.0 * (SUM(o.product_quantity * p.product_price)::numeric / SUM(SUM(o.product_quantity * p.product_price)) OVER ())::numeric, 2) AS "percentage_total(%)"
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
JOIN
    dim_store_details s ON o.store_code = s.store_code
GROUP BY
    s.store_type
ORDER BY
    total_sales DESC;
