v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=dbdev;
date


# CM_table_yesterday loading. Replace existing
v_query_ga_session_table="
select sessionid,platform_type,date,transactionId, TransactionValue from
(SELECT sessionid as sessionid, platform_type as platform_type,date(visit_start_time_ist) as date,null, null 
 from TABLE_DATE_RANGE([big-query-1233:ga_simplified.ga_session_custom_dim_],TIMESTAMP('2016-09-01'), TIMESTAMP (CURRENT_DATE()) ))

,
(
SELECT null, SOURCE, date(createdAt) as date,transactionId, TransactionValue FROM [big-query-1233:BI_Automation.daily_reports_table_CM] 

)"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=ga_session_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_session_table\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_ga_session_table" &
v_first_pid=$!
v_ga_session_table_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_ga_session_table_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_ga_session_table_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "CM table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com 


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


