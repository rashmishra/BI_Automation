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


bq --format=csv query --n=10000000 "select m_location as CityHead,
deal_owner as SalesRep,
m_manager as CityManager,
m_location as CityName,
dealid,
merchantName, 
ol_offertitle,
SOURCE,
categoryid as category,
date(createdat) as Date,
case when DAYOFWEEK(createdat)=1 then WEEK(createdat) -1 else WEEK(createdat) end as week,
case when 
DAYOFWEEK(createdat) = 1 then \"Sunday\"
when DAYOFWEEK(createdat)= 2 then \"Monday\" 
when DAYOFWEEK(createdat)= 3 then \"Tuesday\" 
when DAYOFWEEK(createdat)= 4 then \"Wednesday\" 
when DAYOFWEEK(createdat)= 5 then \"Thursday\" 
when DAYOFWEEK(createdat)= 6 then \"Friday\"
when DAYOFWEEK(createdat)= 7 then \"Saturday\" 
end as weekday, 
exact_count_distinct(orderId) as Transactions , 
sum(TransactionValue) as Bookings, 
sum(GR) as Revenue, 
sum( NumberofVouchers ) as Vouchers, 
sum( creditPoints) as Credits ,
sum(TransactionValue - priceAfterPromo) as Promo
from BI_Automation.daily_reports_table_CM
where m_manager = \"${v_manager_name}\" 
and month(createdat)=${v_querying_month} and year(createdAt)=2017
group by 1,2,3,4,5,6,7,8,9,10,11,12" > /home/ubuntu/BI/data/google_drive/cityManagerReports/2017/${v_month_name}/${v_manager_name}.csv


exit 0
