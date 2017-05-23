
#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

# v_month_name=`date +%b`;
v_month_name=`date -d " -1 day" +%b`;
v_month_number=`date +%m`;
v_querying_month=`date -d " -1 day" +%m`;
v_querying_year=`date -d " -1 day" +%Y`;
v_current_year=`date +%Y`;
v_manager_name=$1;

echo "${v_manager_name} it the Refunds";
echo "${v_month_name} is the Month Name, used in Folder's name";
echo "${v_month_number} is the Month Number, used in the query";


v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive/Refunds/${v_querying_year}/${v_month_name}"


if [ ! -d "${v_grive_folder}" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir ${v_grive_folder} ;
  chmod 0777  ${v_grive_folder} ;
else echo "Directory for the month exists.";
fi


bq --format=csv query --n=10000000 "SELECT refunded_year, refunded_Month, refunded_Day, merchant_name, deal_id, category_id,
city_manager, deal_owner,
sum( refunded_GB) as refunded_GB, sum( GR) as refunded_GR,exact_count_distinct( orderline_id) as refunded_vouchers
,customer_comment
FROM [big-query-1233:nb_reports.refund]
WHERE city_manager = \"${v_manager_name}\"  
  AND refunded_Month=${v_querying_month} 
  AND refunded_year=${v_querying_year}
group by 1,2,3,4,5,6,7,8,12" > ${v_grive_folder}/${v_manager_name}.csv


exit 0

