-- ShipmentOps Analytical Queries

-- Query 1: On-Time Delivery Rate by Warehouse
-- Business Question: Which warehouses have the best/worst on-time delivery performance?
SELECT 
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    COUNT(*) AS total_deliveries,
    SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) AS on_time_deliveries,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 
        2
    ) AS on_time_rate_pct,
    ROUND(AVG(f.days_late), 2) AS avg_days_late
FROM fact_deliveries f
JOIN dim_warehouse w ON f.warehouse_key = w.warehouse_key
WHERE f.is_on_time IS NOT NULL
GROUP BY w.warehouse_id, w.warehouse_name, w.region
ORDER BY on_time_rate_pct DESC;


-- Query 2: Average Delivery Time by Route
-- Business Question: Which routes take the longest? Where should we optimize?
SELECT 
    r.route_id,
    r.origin,
    r.destination,
    r.distance_km,
    COUNT(*) AS delivery_count,
    ROUND(AVG(f.delivery_duration_hours), 2) AS avg_delivery_hours,
    ROUND(AVG(f.days_late), 2) AS avg_days_late,
    ROUND(
        AVG(f.delivery_duration_hours / NULLIF(r.distance_km, 0)), 
        4
    ) AS hours_per_km
FROM fact_deliveries f
JOIN dim_route r ON f.route_key = r.route_key
WHERE f.delivery_duration_hours IS NOT NULL
GROUP BY r.route_id, r.origin, r.destination, r.distance_km
HAVING COUNT(*) >= 5  -- Only routes with meaningful sample size
ORDER BY avg_delivery_hours DESC
LIMIT 20;


-- Query 3: Partner Payment Reconciliation
-- Business Question: Do partner payments match order values? Any discrepancies?
WITH order_totals AS (
    SELECT 
        p.partner_id,
        SUM(f.order_value_usd) AS total_order_value,
        COUNT(DISTINCT f.order_id) AS order_count
    FROM fact_deliveries f
    JOIN dim_partner p ON f.partner_key = p.partner_key
    WHERE f.order_value_usd IS NOT NULL
    GROUP BY p.partner_id
),
payment_totals AS (
    SELECT 
        partner_id,
        SUM(amount_usd) AS total_paid,
        COUNT(*) AS payment_count
    FROM partner_payments_clean
    WHERE amount_usd IS NOT NULL
    GROUP BY partner_id
)
SELECT 
    COALESCE(o.partner_id, p.partner_id) AS partner_id,
    o.total_order_value,
    p.total_paid,
    COALESCE(p.total_paid, 0) - COALESCE(o.total_order_value, 0) AS payment_difference,
    CASE 
        WHEN ABS(COALESCE(p.total_paid, 0) - COALESCE(o.total_order_value, 0)) < 100 THEN 'Balanced'
        WHEN COALESCE(p.total_paid, 0) < COALESCE(o.total_order_value, 0) THEN 'Underpaid'
        ELSE 'Overpaid'
    END AS reconciliation_status,
    o.order_count,
    p.payment_count
FROM order_totals o
FULL OUTER JOIN payment_totals p ON o.partner_id = p.partner_id
ORDER BY ABS(COALESCE(p.total_paid, 0) - COALESCE(o.total_order_value, 0)) DESC;


-- Query 4: Month-over-Month Order Volume Trend
-- Business Question: Is order volume growing? Any seasonal patterns?
WITH monthly_orders AS (
    SELECT 
        d.year,
        d.month,
        TO_CHAR(d.full_date, 'YYYY-MM') AS year_month,
        COUNT(DISTINCT f.order_id) AS order_count,
        SUM(f.order_value_usd) AS total_value,
        ROUND(AVG(f.order_value_usd), 2) AS avg_order_value
    FROM fact_deliveries f
    JOIN dim_date d ON f.order_date_key = d.date_key
    GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM')
)
SELECT 
    year_month,
    order_count,
    total_value,
    avg_order_value,
    LAG(order_count) OVER (ORDER BY year_month) AS prev_month_orders,
    ROUND(
        100.0 * (order_count - LAG(order_count) OVER (ORDER BY year_month)) 
        / NULLIF(LAG(order_count) OVER (ORDER BY year_month), 0), 
        2
    ) AS mom_growth_pct
FROM monthly_orders
ORDER BY year_month DESC;


-- Query 5: Delivery Performance Summary (Bonus - Executive Dashboard)
-- Business Question: Overall KPIs for leadership
SELECT 
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT f.warehouse_key) AS active_warehouses,
    COUNT(DISTINCT f.partner_key) AS active_partners,
    ROUND(SUM(f.order_value_usd), 2) AS total_order_value,
    ROUND(AVG(f.order_value_usd), 2) AS avg_order_value,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) 
        / NULLIF(COUNT(CASE WHEN f.is_on_time IS NOT NULL THEN 1 END), 0),
        2
    ) AS overall_on_time_rate,
    ROUND(AVG(f.days_late), 2) AS avg_days_late
FROM fact_deliveries f;