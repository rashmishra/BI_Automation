
#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

# v_month_name=`date +%b`;
v_month_name=`date -d " -1 day" +%b`;
v_month_number=`date +%m`;
v_querying_month=`date -d " -1 day" +%m`;
v_manager_name=$1;

echo "${v_manager_name} it the Refunds";
echo "${v_month_name} is the Month Name, used in Folder's name";
echo "${v_month_number} is the Month Number, used in the query";


if [ ! -d "/home/ubuntu/BI/data/google_drive/Refunds/2017/${v_month_name}/" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir /home/ubuntu/BI/data/google_drive/Refunds/2017/${v_month_name}/ ;
  chmod 0777 /home/ubuntu/BI/data/google_drive/Refunds/2017/${v_month_name} ;
else echo "Directory for the month exists.";
fi


bq --format=csv query --n=10000000 "SELECT refunded_year, refunded_Month, refunded_Day, merchant_name, deal_id, category_id,
city_manager, deal_owner,
sum( refunded_GB) as refunded_GB, sum( GR) as refunded_GR,exact_count_distinct( orderline_id) as refunded_vouchers
,customer_comment
FROM [big-query-1233:nb_reports.refund]
where city_manager = \"${v_manager_name}\"  and
 refunded_Month=${v_querying_month} and refunded_year=2017
group by 1,2,3,4,5,6,7,8,12" > /home/ubuntu/BI/data/google_drive/Refunds/2017/${v_month_name}/${v_manager_name}.csv


exit 0

