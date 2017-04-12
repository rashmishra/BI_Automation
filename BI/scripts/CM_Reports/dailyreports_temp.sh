#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation
bq --format=csv query --n=10000000 "select m_location as CityHead,
deal_owner as SalesRep,
m_manager as CityManager,
m_location as CityName,
dealid,
merchantName, 
ol_offertitle,
categoryid as category,
date(createdat) as Date,
Week(date(createdat)) as WeekNum,
DAYOFWEEK(createdat) as DayofWeek, 
case when 
DAYOFWEEK(createdat) = 1 then \"Sunday\"
when DAYOFWEEK(createdat)= 2 then \"Monday\" 
when DAYOFWEEK(createdat)= 3 then \"Tuesday\" 
when DAYOFWEEK(createdat)= 4 then \"Wednesday\" 
when DAYOFWEEK(createdat)= 5 then \"Thursday\" 
when DAYOFWEEK(createdat)= 6 then \"Friday\"
when DAYOFWEEK(createdat)= 7 then \"Saturday\" 
end as weekday, 
count( transactionId) as Transactions , 
sum(TransactionValue) as Bookings, 
sum(GR) as Revenue, 
sum( NumberofVouchers ) as Vouchers, 
sum( creditPoints) as Credits ,
sum(TransactionValue - priceAfterPromo) as Promo
from bi.daily_reports_table_CM
where m_manager = \"$1\" 
and month(createdat)=3
group by 1,2,3,4,5,6,7,8,9,10,11,12" > /home/ubuntu/BI/data/google_drive/cityManagerReports/Mar/$1.csv
exit 0








