WITH sales AS (
    SELECT
        orders_id,
        pdt_id,
        quantity,
        revenue
    FROM {{ source('raw', 'sales') }}
),

products AS (
    SELECT
        products_id,
        purchSE_PRICE AS purchase_price
    FROM {{ source('raw', 'products') }}
)

SELECT
    s.orders_id,
    s.pdt_id,
    s.quantity,
    s.revenue,
    p.purchase_price,
    (s.quantity * p.purchase_price) AS purchase_cost, -- Calcul du coût d'achat
    (s.revenue - (s.quantity * p.purchase_price)) AS margin -- Calcul de la marge
FROM sales s
LEFT JOIN products p
    ON s.pdt_id = p.products_id
