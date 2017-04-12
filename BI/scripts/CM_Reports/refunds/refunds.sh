#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

MONTH_NAME=$(date +%b)
echo $MONTH_NAME

echo $1
echo $2

bq --format=csv query --n=10000000 "SELECT refunded_year, refunded_Month, refunded_Day, a_merchantname, a_dealid, a_categoryid,
a_m_manager, a_deal_owner,
sum( refunded_GB) as refunded_GB, sum( GR) as refunded_GR,exact_count_distinct( order_line_id) as refunded_vouchers

FROM [big-query-1233:bi.refunds_table]
where a_m_manager = \"$1\" 
and month(createdat)=$2 and year(createdAt)=2016
group by 1,2,3,4,5,6,7,8" > /home/ubuntu/BI/data/google_drive/cityManagerReports/Refunds/Oct/$1.csv

exit 0

