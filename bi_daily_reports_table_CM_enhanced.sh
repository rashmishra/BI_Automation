v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=adhoc_requests;
date


# CM_table_yesterday loading. Replace existing
v_query_bi_daily_reports_table_CM_enhanced="SELECT dim.calendarweek as calendarweek, DATE(date) as purchase_date_DT 
      , CASE WHEN HOUR(MSEC_TO_TIMESTAMP(rep.createdAt_epoch_ist )) BETWEEN 0 AND 3 THEN 'Slot 1: 00:00 - 03:59'
             WHEN HOUR(MSEC_TO_TIMESTAMP(rep.createdAt_epoch_ist )) BETWEEN 4 AND 7 THEN 'Slot 2: 04:00 - 07:59'
             WHEN HOUR(MSEC_TO_TIMESTAMP(rep.createdAt_epoch_ist )) BETWEEN 8 AND 11 THEN 'Slot 3: 08:00 - 11:59'
             WHEN HOUR(MSEC_TO_TIMESTAMP(rep.createdAt_epoch_ist )) BETWEEN 12 AND 15 THEN 'Slot 4: 12:00 - 15:59'
             WHEN HOUR(MSEC_TO_TIMESTAMP(rep.createdAt_epoch_ist )) BETWEEN 16 AND 19 THEN 'Slot 5: 16:00 - 19:59'
             WHEN HOUR(MSEC_TO_TIMESTAMP(rep.createdAt_epoch_ist )) BETWEEN 20 AND 23 THEN 'Slot 6: 20:00 - 23:59'
         END as Purchase_Slot
      , rep.*
FROM
(
SELECT *
       , TIMESTAMP_TO_MSEC(createdAt) as createdAt_epoch_ist
       , TIMESTAMP_TO_MSEC(createdAt) - TIMESTAMP_TO_MSEC(createdAt)%86400000 as rounded_date_epoch 
       FROM [big-query-1233:bi.daily_reports_table_CM]
) rep
LEFT JOIN [dbdev.date_dimension] dim ON rep.rounded_date_epoch = dim.epoch_gmt"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=bi_daily_reports_table_CM_enhanced
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_bi_daily_reports_table_CM_enhanced\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_bi_daily_reports_table_CM_enhanced" &
v_first_pid=$!
v_CM_Tbl_pids+=" $v_first_pid"
wait $v_first_pid;



if wait $v_CM_Tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_CM_Tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "bi_daily_reports_table_CM_enhanced table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


