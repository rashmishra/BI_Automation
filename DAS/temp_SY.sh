v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=temp;
date


# query for the new table
v_query_appsflyer_processed_table="select * from nb_reports.master_transaction limit 100"


tableName=temp_SY


v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_appsflyer_processed_table\""


bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_appsflyer_processed_table" &
v_first_pid=$!
v_Apps_tbl_pids +=" $v_first_pid"
wait $v_first_pid;

//check if all query ran Successfully 
if wait $v_Apps_tbl_pids; 
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_Apps_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Appsflyer table status:$v_table_status`date`" | mail -s "$v_table_status" sudarshan.yadav@nearbuy.com 


exit 0


