#!/bin/bash

v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date



# scheduling_redemption loading. Replace existing
v_query_redemption="select * from BI_Automation.sales_rep_mapping"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=sales_rep_mapping
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_redemption\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_redemption" &
v_first_pid=$!
v_BI_sdl_pids+=" $v_first_pid"

wait $v_first_pid;


if wait $v_BI_sdl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"

if wait $v_BI_sdl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Table refresh status of sales_rep_mapping table, cancellation table and redemption table in BI_Automation dataset: $v_table_status `date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com 

exit 0

