#!/bin/bash
TASK_START_TIME=`date`
echo $TASK_START_TIME

bq query --append=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.txns_ios_All "SELECT A.region, A.city, A.transactionid, A.transactions, B.destination,CASE WHEN B.destination IS NULL THEN \"Rest_of_World\" ELSE B.destination END New_Destination FROM bi.ios_txns_in_GA_yesterday A LEFT JOIN [bi.GA_city_region_mapping] B ON A.city = B.city AND A.region = B.region group by 1,2,3,4,5,6"

bq query --append=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.txns_android_All "SELECT A.region, A.city, A.transactionid, A.transactions, B.destination,CASE WHEN B.destination IS NULL THEN \"Rest_of_World\" ELSE B.destination END New_Destination FROM bi.android_txns_in_GA_yesterday A LEFT JOIN [bi.GA_city_region_mapping] B ON A.city = B.city AND A.region = B.region group by 1,2,3,4,5,6"

bq query --append=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.txns_web_All "SELECT A.device_category, A.region, A.city, A.transactionid, A.transactions, B.destination,CASE WHEN B.destination IS NULL THEN \"Rest_of_World\" ELSE B.destination END New_Destination FROM bi.web_txns_in_GA_yesterday A LEFT JOIN [bi.GA_city_region_mapping] B ON A.city = B.city AND A.region = B.region group by 1,2,3,4,5,6,7"

bq query --append=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.traffic_web_all "SELECT A.device_category, A.region, A.city, A.dcg, A.sessions, A.bounce_rate,string(year(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_year,string(month(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_month,string(day(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_day,B.destination as B_destination,CASE WHEN B.destination IS NULL THEN \"Rest_of_World\" ELSE B.destination END New_Destination FROM [bi.traffic_web_yesterday] A LEFT JOIN [bi.GA_city_region_mapping] B ON A.city = B.city AND A.region = B.region"

bq query --append=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.traffic_android_all "SELECT A.region, A.city, A.dcg, A.sessions, A.exit_rate,string(year(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_year,string(month(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_month,string(day(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_day,
B.destination as B_destination,CASE WHEN B.destination IS NULL THEN \"Rest_of_World\" ELSE B.destination END New_Destination FROM [bi.traffic_android_yesterday] A LEFT JOIN [bi.GA_city_region_mapping] B
ON A.city = B.city AND A.region = B.region"

bq query --append=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.traffic_ios_all "SELECT A.region, A.city, A.dcg, A.sessions, A.exit_rate,string(year(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_year,string(month(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_month,string(day(STRFTIME_UTC_USEC(now()-86400000000, \"%Y-%m-%d\"))) as A_day,B.destination as B_destination,CASE WHEN B.destination IS NULL THEN \"Rest_of_World\" ELSE B.destination END New_Destination FROM [bi.traffic_ios_yesterday] A LEFT JOIN [bi.GA_city_region_mapping] B
ON A.city = B.city AND A.region = B.region"

bq query --replace=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.sales_rep_mapping1 "select * from bi.sales_rep_mapping"

bq query --replace=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.buy_price2 "select * from bi.BUY_PRICE1"

bq query --replace=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.refund_responses1 "select * from bi.refund_responses"

bq query --replace=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.travel_category1 "select * from bi.travel_category"



TASK_END_TIME=`date`
echo $TASK_END_TIME

mutt -a /home/ubuntu/BI/logs/Akhil_logging.log -s "Here are your logs"  -- akhil.dhingra@nearbuy.com,rahul.sachan@nearbuy.com < /dev/null
