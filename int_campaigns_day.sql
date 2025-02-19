SELECT
date_date
,SUM(COALESCE(ads_cost,0)) AS ads_cost
,SUM(COALESCE(impression,0)) AS impression
,SUM(COALESCE(click,O)) AS click
FROM {{ ref('int_campaigns') }}
GROUP BY date_date
ORDER BY date_date DESC