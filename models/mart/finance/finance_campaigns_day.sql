SELECT
date_date
,SUM(operational_margin-ads_cost) AS ads_margin
,average_basket
,operational_margin
,ads_cost
,ads_impression
,ads_clicks
,quantity
,revenue
,purchase_cost
,margin
,shipping_fee
,logcost
,ship_cost
FROM {{ ref('finance_days') }}
LEFT JOIN {{ ref('int_campaigns_day2') }}
GROUP BY date_date