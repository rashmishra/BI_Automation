v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date


# Master_Transaction table loading. Replace existing
v_query_credits_expired="select 
Date(msec_to_timestamp(cd.createdat+19800000)) as credit_expiry_Date,
cd.userid as customerid, 
c.name as Customer_Name, 
c.primaryEmailAddress as customer_EmailAddress, 
c.primaryPhoneNumber as customer_PhoneNumber ,
sum(cd.credits/100) as credits
from  [big-query-1233:Atom.user_credit_detail] cd
left join [big-query-1233:Atom.customer] c on c.customerid = cd.userid 
where cd.eventtype = 'expired'  and Date(msec_to_timestamp(cd.createdat+19800000)) >='2017-03-01'
group by 1,2,3,4,5
order by 1 desc    
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=credits_expired
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_credits_expired\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_credits_expired" &
v_first_pid=$!
v_txn_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_txn_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_txn_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "credits_expired Table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


