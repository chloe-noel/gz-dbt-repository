SELECT
date_date
,SUM(operational_margin-ads_cost) AS ads_margin
,AVG(average_basket) AS average_basket
,SUM(operational_margin) AS operational_margin
,SUM(ads_cost) AS ads_cost
,COUNT(impression) AS impression
,COUNT(click) AS click
,COUNT(quantity) AS quantity
,SUM(revenue) AS revenue
,SUM(purchase_cost) AS purchase_cost
,SUM(margin) AS margin
,SUM(shipping_fee) AS shipping_fee
,SUM(logcost) AS logcost
,SUM(ship_cost) AS ship_cost
FROM {{ ref('finance_days') }}
LEFT JOIN {{ ref('int_campaigns_day2') }}
USING (date_date)
GROUP BY date_date