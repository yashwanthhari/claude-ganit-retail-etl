-- Daily sales KPIs with a 7-day moving average (window function practice).
CREATE OR REPLACE TABLE marts.daily_sales AS
WITH daily AS (
    SELECT
        order_date,
        COUNT(DISTINCT order_id) AS orders,
        SUM(qty)                 AS units,
        SUM(gross_amount)        AS gross_sales,
        SUM(discount_amount)     AS discounts,
        SUM(net_amount)          AS net_sales
    FROM core.fact_sales
    WHERE NOT is_cancelled
    GROUP BY order_date
)
SELECT
    order_date,
    orders,
    units,
    gross_sales,
    discounts,
    net_sales,
    ROUND(net_sales / NULLIF(orders, 0), 2) AS avg_order_value,
    ROUND(AVG(net_sales) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS net_sales_7d_avg
FROM daily
ORDER BY order_date;
