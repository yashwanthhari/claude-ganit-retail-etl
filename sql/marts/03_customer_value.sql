-- Customer value with simple RFM quartile scores (4 = best).
CREATE OR REPLACE TABLE marts.customer_value AS
WITH per_customer AS (
    SELECT
        customer_key,
        COUNT(DISTINCT order_id) AS orders,
        SUM(net_amount)          AS net_sales,
        MIN(order_date)          AS first_order_date,
        MAX(order_date)          AS last_order_date
    FROM core.fact_sales
    WHERE NOT is_cancelled
    GROUP BY customer_key
),
anchor AS (
    SELECT MAX(order_date) AS as_of FROM core.fact_sales
),
base AS (
    SELECT
        c.customer_id,
        c.name,
        c.city,
        c.segment,
        p.orders,
        p.net_sales,
        p.first_order_date,
        p.last_order_date,
        date_diff('day', p.last_order_date, a.as_of) AS recency_days
    FROM per_customer AS p
    JOIN core.dim_customer AS c ON c.customer_key = p.customer_key
    CROSS JOIN anchor AS a
)
SELECT
    *,
    NTILE(4) OVER (ORDER BY recency_days DESC) AS r_score,
    NTILE(4) OVER (ORDER BY orders)            AS f_score,
    NTILE(4) OVER (ORDER BY net_sales)         AS m_score
FROM base;
