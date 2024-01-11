WITH MonthlySales AS (
    SELECT
        d.year AS year,
        d.month AS month,
        ROUND(SUM(o.product_quantity * p.product_price)::numeric, 2) AS total_sales
    FROM
        orders_table o
	JOIN 
		dim_date_times d ON o.date_uuid = d.date_uuid
    JOIN
        dim_products p ON o.product_code = p.product_code
    GROUP BY
        year, month
)
SELECT
    total_sales,
    year,
    month
FROM (
    SELECT
        total_sales,
        year,
        month,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_sales DESC) AS rn
    FROM
        MonthlySales
) ranked
WHERE
    rn = 1
ORDER BY
    total_sales DESC
LIMIT 
	9;