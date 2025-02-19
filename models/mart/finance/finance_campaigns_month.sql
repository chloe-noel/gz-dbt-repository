SELECT
DATE_TRUNC(date_date, month) AS date_month
,SUM(operational_margin-ads_cost) AS ads_margin
,ROUND(AVG(average_basket),2) AS average_basket
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
GROUP BY date_month
ORDER BY date_month DESC