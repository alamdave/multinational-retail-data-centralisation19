/* The business stakeholders would like to know which locations currently have the most stores.

They would like to close some stores before opening more in other locations.

Find out which locations have the most stores currently. The query should return the following: */

SELECT 
    locality,
    COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;