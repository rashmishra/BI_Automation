

####Load 10day-orders-report.csv from Google Cloud to BigQuery Dataset "Little_staging" 

bq load --replace --quiet --field_delimiter='^' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1 Little_staging.orders gs://little_aws/reports/txns/10day-orders-report.csv schema_order.json


v_first_pid=$!
v_order_tbl_pids+=" $v_first_pid"
wait $v_first_pid;

####Load 10day-users-report.csv from Google Cloud to BigQuery Dataset "Little_staging" 

bq load --replace --quiet --field_delimiter='^' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1 Little_staging.user gs://little_aws/reports/txns/10day-users-report.csv schema_user.json

v_first_pid=$!
v_order_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


####Load 30day-refund-report.csv from Google Cloud to BigQuery Dataset "Little_staging" 

bq load --replace --quiet --field_delimiter='^' --source_format=CSV --skip_leading_rows=1 --max_bad_records=0  --allow_jagged_rows=1 --allow_quoted_newlines=1 --ignore_unknown_values=1 Little_staging.refund gs://little_aws/reports/txns/30day-refund-report.csv schema_refund.json

v_first_pid=$!
v_order_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


####Load Yesterday Data of Order table from "Little_staging" to Little Dataset


v_dataset_name=Little;

v_query_orders="SELECT * FROM [big-query-1233:Little_staging.orders] 
where Date(msec_to_timestamp(created_at*1000+19800000)) = date(date_add(current_date(), -1,'day'))"

tableName=orders
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_orders\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_orders" &
v_first_pid=$!
v_order_pids+=" $!"
wait $v_first_pid;


v_first_pid=$!
v_order_tbl_pids+=" $v_first_pid"
wait $v_first_pid;

####Load Yesterday Data of User table from "Little_staging" to Little Dataset


v_query_users="SELECT * FROM [big-query-1233:Little_staging.user] 
where Date(msec_to_timestamp(created_at*1000+19800000)) = date(date_add(current_date(), -1,'day'))"

tableName=users
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_user\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_user" &
v_first_pid=$!
v_order_pids+=" $!"
wait $v_first_pid;


####Load Yesterday Data of Refund table from "Little_staging" to Little Dataset


v_query_refund="SELECT * FROM [big-query-1233:Little_staging.refund] 
where Date(msec_to_timestamp(created_at*1000+19800000)) = date(date_add(current_date(), -1,'day'))"

tableName=refunds
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_refund\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_refund" &
v_first_pid=$!
v_order_pids+=" $!"
wait $v_first_pid;



if wait $v_order_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_order_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo " Little Order Table and User Table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com



exit 0




