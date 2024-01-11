/* The company is looking to increase its online sales.

They want to know how many sales are happening online vs offline.

Calculate how many products were sold and the amount of sales made for online and offline purchases.

You should get the following information: */


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
