#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

# v_month_name=`date +%b`;
v_month_name=`date -d " -1 day" +%b`;
v_month_number=`date +%m`;
v_querying_month=`date -d " -1 day" +%m`;
v_manager_name=$1;

echo "${v_manager_name} it the City Manager";
echo "${v_month_name} is the Month Name, used in Folder's name";
echo "${v_month_number} is the Month Number, used in the query";


if [ ! -d "/home/ubuntu/BI/data/google_drive/cityManagerReports/2017/${v_month_name}/" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir /home/ubuntu/BI/data/google_drive/cityManagerReports/2017/${v_month_name}/ ;
  chmod 0777 /home/ubuntu/BI/data/google_drive/cityManagerReports/2017/${v_month_name} ;
else echo "Directory for the month exists.";
fi


bq --format=csv query --n=10000000 "select
 cm_location as CityHead,
deal_owner as SalesRep,
city_manager as CityManager,
cm_location as CityName,
deal_id,
merchant_Name, 
offer_title,
platform_type ,
category_id as category,
date( date_time_ist ) as Date,
case when DAYOFWEEK( date_time_ist )=1 then WEEK(date_time_ist) -1 else WEEK( date_time_ist ) end as week,
case when 
DAYOFWEEK( date_time_ist ) = 1 then 'Sunday'
when DAYOFWEEK( date_time_ist )= 2 then 'Monday' 
when DAYOFWEEK( date_time_ist )= 3 then 'Tuesday'
when DAYOFWEEK( date_time_ist )= 4 then 'Wednesday'
when DAYOFWEEK( date_time_ist )= 5 then 'Thursday'
when DAYOFWEEK( date_time_ist )= 6 then 'Friday'
when DAYOFWEEK( date_time_ist )= 7 then 'Saturday'
end as weekday, 
exact_count_distinct(order_Id) as Transactions , 
sum(GB) as Bookings, 
sum(GR) as Revenue, 
sum( number_of_vouchers ) as Vouchers, 
sum( credits_requested ) as Credits ,
sum( GB - price_after_promo ) as Promo
from nb_reports.master_transaction
where --date(date_time_ist) between '2017-03-01' and '2017-03-20'
city_manager = \"${v_manager_name}\"  and
month( date_time_ist )=${v_querying_month} and year( date_time_ist )=2017
group by 1,2,3,4,5,6,7,8,9,10,11,12" > /home/ubuntu/BI/data/google_drive/cityManagerReports/2017/${v_month_name}/${v_manager_name}.csv


exit 0
