#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

MONTH_NAME=$(date +%b)
echo $MONTH_NAME

echo $1
echo $2

v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive/deals_live"

bq --format=csv query --n=10000000 "select  dealId, m.merchant_name, m.deal_owner, s.manager, min(date) as deal_live_in_this_month_since,deal_end_date, category
from
(SELECT
  y.date AS date,
  y.Deal_ID AS dealId,
  'na' AS counter_value,
  m.merchant_name,
  m.deal_owner,
  s.manager,
  y.Deal_End_Date AS deal_end_date,
  m.deal_category AS category
FROM
  [BI_Automation.live_deals_history_since_15_sep] y
INNER JOIN
  bi.deal_to_merchant m
ON
  m.deal_Id = y.Deal_ID
LEFT OUTER JOIN
  bi.sales_rep_mapping1 s
ON
  m.deal_owner = s.sales_rep
WHERE
  month(date) = month(STRFTIME_UTC_USEC(NOW()-86400000000, \"%Y-%m-%d\"))
  and year(date) = year(STRFTIME_UTC_USEC(NOW()-86400000000, \"%Y-%m-%d\"))
GROUP BY
  date,
  dealId,
  counter_value,
  deal_end_date,
  m.merchant_name,
  m.deal_owner,
  s.manager,
  category)
  group by 1,2,3,4,6,7" > ${v_grive_folder}/deals_live.csv
  # /home/ubuntu/BI/data/google_drive/deals_live/deals_live.csv

exit 0

