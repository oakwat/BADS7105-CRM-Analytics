WITH 

-- create a table for every customer for all months using cross join
customer_alldate AS 
    (
        SELECT 
            CUST_CODE,
            year_month 
        FROM (
            SELECT FORMAT_DATETIME("%Y-%m", master_date) AS year_month
            FROM UNNEST(
            GENERATE_DATE_ARRAY(
                (SELECT MIN(SHOP_DATE) FROM 
                    (SELECT PARSE_DATE('%Y%m%d', CAST(SHOP_DATE AS STRING)) AS SHOP_DATE,
                        FROM`axiomatic-grove-331409.Supermarket2.supermarket` 
                            WHERE CUST_CODE IS NOT NULL)), 
                (SELECT DATE_ADD(MAX(SHOP_DATE),INTERVAL 1 MONTH) FROM 
                    (SELECT PARSE_DATE('%Y%m%d', CAST(SHOP_DATE AS STRING)) AS SHOP_DATE,
                        FROM`axiomatic-grove-331409.Supermarket2.supermarket` 
                            WHERE CUST_CODE IS NOT NULL)), INTERVAL 1 MONTH)) AS master_date )
        CROSS JOIN
            (SELECT DISTINCT(CUST_CODE),
            FROM `axiomatic-grove-331409.Supermarket2.supermarket` 
            WHERE CUST_CODE IS NOT NULL)
        ) ,

-- create customer table where txn occured groupby into monthly level
customer_table AS
    (
        SELECT 
            CUST_CODE,
            FORMAT_DATETIME("%Y-%m", (PARSE_DATE('%Y%m%d', CAST(SHOP_DATE AS STRING)))) AS year_month,
        FROM`axiomatic-grove-331409.Supermarket2.supermarket` 
        WHERE CUST_CODE IS NOT NULL
        GROUP BY CUST_CODE, year_month
    ) ,

-- combine the first two tables to fill in missing months for each customer
combined_table AS 
    (
        SELECT
            customer_alldate.CUST_CODE AS CUST_CODE1,
            customer_table.CUST_CODE AS CUST_CODE2,
            customer_alldate.year_month AS year_month1,
            customer_table.year_month AS year_month2,
        FROM customer_alldate
        LEFT JOIN customer_table 
        ON customer_alldate.CUST_CODE = customer_table.CUST_CODE AND customer_alldate.year_month = customer_table.year_month
        ORDER BY CUST_CODE1,year_month1
    ) ,

-- create last visited month lag, calculate days from last visit month, and flag first purchased month 
lag_combined_table AS 
    (
        SELECT *,
            LAG(year_month2) OVER (PARTITION BY CUST_CODE1 ORDER BY year_month2) AS prev_month,
            DATE_DIFF(DATETIME(CONCAT(year_month2,"-01")), DATETIME(CONCAT((LAG(year_month2) OVER (PARTITION BY CUST_CODE1 ORDER BY year_month2)),"-01")), DAY) AS days,
            MIN(year_month2) OVER (PARTITION BY CUST_CODE1 ORDER BY year_month1 
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS filled_min_date
        FROM combined_table
        ORDER BY CUST_CODE1,year_month1
    )

-- Flag status and numerical value and removing months prior to first purchase
SELECT * ,
    CASE
        WHEN prev_month IS NULL AND year_month2 IS NOT NULL THEN "New"
        WHEN days IS NULL THEN "Churn"
        WHEN days <= 31 THEN "Active"
        WHEN days > 31 THEN "Reactivate"
        ELSE "Churn"
    END AS status,
    CASE
        WHEN prev_month IS NULL AND year_month2 IS NOT NULL THEN 1
        WHEN days IS NULL THEN -1
        WHEN days <= 31 THEN 1
        WHEN days > 31 THEN 1
        ELSE -1
    END AS status_flag
FROM lag_combined_table 
WHERE filled_min_date IS NOT NULL
ORDER BY CUST_CODE1,year_month1

