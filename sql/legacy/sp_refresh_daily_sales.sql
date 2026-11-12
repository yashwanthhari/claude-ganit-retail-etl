-- EXERCISE INPUT (not executed by the pipeline).
-- A legacy SQL Server stored procedure of the kind consultancies migrate to Spark/Databricks.
-- Task: re-implement it as a PySpark job reading silver and writing a Delta table,
-- then compare its output with marts.daily_sales. See docs/exercises.md (exercise 6).

CREATE PROCEDURE dbo.sp_refresh_daily_sales @RunDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. staging table with the latest version of each order line
    IF OBJECT_ID('tempdb..#latest') IS NOT NULL DROP TABLE #latest;

    SELECT oi.order_id, oi.line_no, o.order_date, oi.qty, oi.unit_price, oi.discount_pct, o.status
    INTO #latest
    FROM dbo.order_items oi
    JOIN dbo.orders o ON o.order_id = oi.order_id
    WHERE o.order_date = @RunDate;

    -- 2. remove the day being refreshed
    DELETE FROM dbo.daily_sales WHERE order_date = @RunDate;

    -- 3. recompute the day, excluding cancelled orders
    INSERT INTO dbo.daily_sales (order_date, orders, units, net_sales)
    SELECT order_date,
           COUNT(DISTINCT order_id),
           SUM(qty),
           SUM(qty * unit_price * (1 - discount_pct / 100.0))
    FROM #latest
    WHERE status <> 'cancelled'
    GROUP BY order_date;
END
