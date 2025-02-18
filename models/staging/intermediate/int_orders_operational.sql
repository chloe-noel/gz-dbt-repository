WITH sales_margin AS (
    SELECT * FROM {{ ref('int_sales_margin') }}
),
ship_costs AS (
    SELECT
        orders_id,
        shipping_fee,
        log_cost,
        ship_cost
    FROM {{ source('raw', 'ship') }}
)

SELECT
    sm.orders_id,
    MIN(sm.date_date) AS date_date,  -- Assurer une seule date par commande
    SUM(sm.margin + sc.shipping_fee - sc.log_cost - sc.ship_cost) AS operating_margin
FROM sales_margin sm
LEFT JOIN ship_costs sc
    ON sm.orders_id = sc.orders_id
GROUP BY sm.orders_id