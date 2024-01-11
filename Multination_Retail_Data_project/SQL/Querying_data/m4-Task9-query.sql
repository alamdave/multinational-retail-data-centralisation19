-- CTE 1: Create a table combining day, month, year, and timestamp
WITH CTE1 AS (
    SELECT year,
           TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', time), 'YYYY-MM-DD HH24:MI:SS') AS event_time
    FROM dim_date_times
),

-- CTE 2: Create a table providing the next time in the event_time column
CTE2 AS (
    SELECT year, event_time, LEAD(event_time) OVER (PARTITION BY year ORDER BY event_time) AS next_event_time
    FROM CTE1
),

-- CTE 3: Compute the average time in seconds
CTE3 AS (
    SELECT year, AVG(EXTRACT(EPOCH FROM (next_event_time - event_time))) AS average_time_seconds
    FROM CTE2
    GROUP BY year
)

-- Main query to retrieve the average time in the desired format
SELECT
    year,
    CONCAT(
        '"hours": ', FLOOR(average_time_seconds / 3600), ', ',
        '"minutes": ', FLOOR(MOD(average_time_seconds / 60, 60)), ', ',
        '"seconds": ', FLOOR(MOD(average_time_seconds, 60)), ', ',
        '"milliseconds": ', ROUND(MOD(average_time_seconds, 1) * 1000)
    ) AS average_time_formatted
FROM CTE3
ORDER BY average_time_seconds DESC
LIMIT 5;