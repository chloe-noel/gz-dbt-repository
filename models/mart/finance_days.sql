
WITH orders_margin AS (
    SELECT * FROM {{ ref('int_orders_margin') }}
),
ship_costs AS (
    SELECT
        orders_id,
        shipping_fee,
        log_cost
    FROM {{ source('raw', 'ship') }}
)

SELECT
    o.date_date AS date,
    COUNT(o.orders_id) AS total_transactions,
    SUM(o.revenue) AS total_revenue,
    SUM(o.revenue) / NULLIF(COUNT(o.orders_id), 0) AS avg_basket,
    SUM(o.operating_margin) AS operating_margin,
    SUM(o.purchase_cost) AS total_purchase_cost,
    SUM(s.shipping_fee) AS total_shipping_fee,
    SUM(s.log_cost) AS total_log_cost,
    SUM(o.quantity) AS total_quantity_sold
FROM orders_margin o
LEFT JOIN ship_costs s
    ON o.orders_id = s.orders_id
GROUP BY o.date_date
ORDER BY o.date_date