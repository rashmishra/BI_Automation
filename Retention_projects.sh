v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=Projects;
date

v_query_Retention_projects="select acq.*, txn.*
 
from 
 
(select
customer_id,
category_id,
first_purchase_date,
platform_type,
sum(GB) as GB,
count(order_id) as vouchers,
sum(GR -(GB - price_after_promo)) as Net_Revenue,
sum(GR) as GR1,
sum(credits_requested) as credits,
merchant_name,
string(order_id) as norderid,
deal_id,
cm_location,
city_manager,
deal_owner
from nb_reports.master_transaction where first_transaction ='TRUE'

group each by 1,2,3,4,10,11,12,13,14,15) acq
 
left join
 
(select customer_id,
order_id,
date_time_ist,
platform_type,
merchant_Name,
category_id,
sum(GB) as GB,
count(order_Id) as vouchers,
exact_count_distinct(order_Id) as txns,
sum(GR -(GB - price_after_promo)) as Net_Revenue,
sum(GR) as GR1 ,
sum(credits_requested) as credits
from nb_reports.master_transaction group by 1,2,3,4,5,6) as txn
 
on txn.customer_id = acq.customer_id
 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=Retention_projects
v_destination_tbl="Projects.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_Retention_projects\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_Retention_projects" &
v_first_pid=$!
v_Retention_projects_pids+=" $v_first_pid"
wait $v_first_pid;

if wait $v_Retention_projects_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_Retention_projects_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in table loads" ;
fi

echo "Retention_projects table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ritwik.mallik@nearbuy.com 


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0

