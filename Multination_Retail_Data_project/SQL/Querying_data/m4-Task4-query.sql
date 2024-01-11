-- Online and Offline Sales with "WEB" store code prefix
SELECT
    COUNT(*) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count
FROM
    orders_table o
WHERE
    o.store_code LIKE 'WEB%'

UNION

SELECT
    COUNT(*) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count
FROM
    orders_table o
WHERE
    o.store_code NOT LIKE 'WEB%';
