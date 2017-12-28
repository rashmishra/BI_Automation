v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=DAS;
date


# CM_table_yesterday loading. Replace existing
v_query_FNB_keymerchants_traffic="select Today_date, Merchant_Name, Deal_ID,City,Zone,Status,
sum(DD_Views) Deal_Detail_Views,
sum(Buy_Nows) Buy_Nows, sum(Transactions) Transactions from 

(SELECT Merchant_Name,Deal_ID,City,
Zone,Status FROM [big-query-1233:DAS.FNB_keymerchants] ) as a

left join 

(select DATE(date) Today_date,
hits.product.productSKU dealid, 
sum(case when hits.eCommerceAction.action_type='2' then 1
else 0 end ) DD_Views,
sum(case when hits.eCommerceAction.action_type='3' then 1
else 0 end ) Buy_Nows,
exact_count_distinct( hits.transaction.transactionId) Transactions


FROM
    TABLE_DATE_RANGE([124486161.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(),-1,'Month')),
    TIMESTAMP (CURRENT_DATE()))
    
  where  hits.product.v2ProductCategory='FNB'
  group by 1,2) as b
  
  on a.Deal_ID=b.dealid
  
  group by 1,2,3,4,5,6
  
 order by 1 desc
"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=FNB_keymerchants_traffic 
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_FNB_keymerchants_traffic\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_FNB_keymerchants_traffic" &
v_first_pid=$!
v_deals_live_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_deals_live_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"


if wait $v_deals_live_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo " FNB_keymerchants_traffic Table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


