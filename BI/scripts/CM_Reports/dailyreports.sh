#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

# v_month_name=`date +%b`;
v_month_name=`date -d " -1 day" +%b`;
v_month_number=`date +%m`;
v_querying_month=`date -d " -1 day" +%m`;
v_manager_name=$1;

v_year=`date -d " -1 day" +%Y`

v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive/cityManagerReports"

echo "${v_manager_name} it the City Manager";
echo "${v_month_name} is the Month Name, used in Folder's name";
echo "${v_month_number} is the Month Number, used in the query";


if [ ! -d "${v_grive_folder}/${v_year}/${v_month_name}/" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir "${v_grive_folder}/${v_year}/${v_month_name}/" ;
  chmod 0777 "${v_grive_folder}/${v_year}/${v_month_name}" ;
else echo `date` " Directory for the month ${v_month_name} exists.";
fi


bq --format=csv query --n=10000000 "select
 cm_location as CityHead,
deal_owner as SalesRep,
city_manager as CityManager,
cm_location as CityName,
a.deal_id as deal_id,
c.city_name as deal_city,
c.state_name as deal_state,
c.zone as deal_Region,
a.merchant_Name as merchant_Name, 
offer_title,
platform_type ,
category_id as category,
b.category as Secondry_category,
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
Round(sum(GR),2) as Revenue, 
sum( number_of_vouchers ) as Vouchers, 
Round(sum( credits_requested ),2) as Credits ,
sum( GB - price_after_promo ) as Promo,
Round(sum(GR - ifnull((GR_refunded),0)),2) as Net_GR
from (
select 
order_id,
GB, 
GR,    
cm_location ,
deal_owner ,
city_manager ,
deal_id,
merchant_Name, 
offer_title,
platform_type ,
category_id,
date_time_ist,
number_of_vouchers,
price_after_promo,
credits_requested, merchant_id,
case when orderline_status in ('Refund','Cancelled') then GR end GR_refunded
from nb_reports.master_transaction 
) a
left join bi.travel_category1 b on a.deal_id = b.deal_ID
left join (select  string(merchantid) as merchantid,redemptionAddress.cityTown as city, redemptionAddress.state as state  from Atom.merchant WHERE isPublished = true
          GROUP BY 1, 3, 2) m on m.merchantid = a.merchant_id
left join BI_Automation.city_state_mapping c on c.city_name = m.city
where 
city_manager = \"${v_manager_name}\"  and
month( date_time_ist )=${v_querying_month} and year( date_time_ist )=2017
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
" > ${v_grive_folder}/${v_year}/${v_month_name}/${v_manager_name}.csv


exit 0
