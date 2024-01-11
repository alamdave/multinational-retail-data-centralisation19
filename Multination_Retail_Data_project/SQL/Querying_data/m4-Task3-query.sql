SELECT
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
    dim_date_times.month AS month
FROM
    orders_table
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
    dim_date_times.month
ORDER BY
    total_sales DESC
LIMIT 6;