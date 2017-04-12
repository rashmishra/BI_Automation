#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

MONTH_NAME=$(date +%b)
echo $MONTH_NAME

echo $1
echo $2

bq --format=csv query --n=10000000 "SELECT cancellation_year , cancellation_month , cancellation_day , b_merchantname , b_dealid , b_categoryid ,
manager , deal_Owner ,
sum( cancelled_GB ) as Cancelled_GB, sum( GR) as Cancelled_GR,exact_count_distinct( order_line_id) as Cancelled_vouchers

FROM [big-query-1233:bi.Cancellation]
where cancellation_year = 2016 and cancellation_month =$2
and manager = \"$1\" 
group by 1,2,3,4,5,6,7,8" > /home/ubuntu/BI/data/google_drive/cityManagerReports/Cancellations/Oct/$1.csv

exit 0

