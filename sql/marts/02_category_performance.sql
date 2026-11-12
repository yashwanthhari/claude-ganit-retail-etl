-- Monthly category performance with a rank inside each month.
CREATE OR REPLACE TABLE marts.category_performance AS
WITH monthly AS (
    SELECT
        date_trunc('month', f.order_date) AS month,
        p.category,
        SUM(f.net_amount)      AS net_sales,
        SUM(f.gross_amount)    AS gross_sales,
        SUM(f.discount_amount) AS discounts,
        SUM(f.qty)             AS units
    FROM core.fact_sales AS f
    JOIN core.dim_product AS p ON p.product_key = f.product_key
    WHERE NOT f.is_cancelled
    GROUP BY 1, 2
)
SELECT
    month,
    category,
    net_sales,
    units,
    ROUND(100.0 * discounts / NULLIF(gross_sales, 0), 2) AS discount_rate_pct,
    RANK() OVER (PARTITION BY month ORDER BY net_sales DESC) AS sales_rank
FROM monthly
ORDER BY month, sales_rank;
