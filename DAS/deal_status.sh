v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date

v_query_offline="select
date(date_add(current_date(),-1,'day')) as Date, 
deal_id, 
Category
from [BI_Automation.live_deals_history_since_15_sep] 
where  date(Date ) = date(date_add(current_date(),-2,'day'))
and deal_id not in 
(
select
deal_id
from [BI_Automation.live_deals_history_since_15_sep] 
where  date(Date) = date(date_add(current_date(),-1,'day'))
)
group by 1,2,3
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=offline_deals_since_1feb2018
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_offline\""
bq query --maximum_billing_tier 10000 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_offline" &
v_first_pid=$!
v_downstream_pids+=" $v_first_pid"
wait $v_first_pid;

# Downstream table loading. Replace existing
v_query_madelive="select
date(date_add(current_date(),-1,'day')) as Date,
Deal_ID , 
Category 
from [BI_Automation.live_deals_history_since_15_sep] 

where 

date(date) = date(date_add(current_date(),-1,'day'))
and deal_id not in 
(
select  deal_id   from [BI_Automation.live_deals_history_since_15_sep] 
where date(date) < date(date_add(current_date(),-1,'day'))
)
group by 1,2,3
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=deals_madelive_firsttime_since1feb2018
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_madelive\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_madelive" &
v_first_pid=$!
v_deals_pids+=" $v_first_pid"
wait $v_first_pid;

# reengagement loading. Replace existing
v_query_submit="select 
date(msec_to_timestamp(updateHistory.submittedAt+19800000)) as Date
,_id as deal_id
 ,
categoryId 
from [Atom.deal] 
where 
date(msec_to_timestamp(updateHistory.submittedAt+19800000))  = date(date_add(current_date(),-1,'day'))
and flags.isSubmitted = true
group by 1,2,3"

tableName=deal_sumitted_since_1feb2018
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_submit\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_submit" &
##v_first_pid=$!
v_deals_pids+=" $!"


if wait $v_deals_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_deals_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Deals offline, made live ans sumbit  Tables status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ## sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


