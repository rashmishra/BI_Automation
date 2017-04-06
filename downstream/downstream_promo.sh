v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=bi;
date


# downstream_promo loading. Replace existing
v_query_downstream_promo="SELECT a.*,b.* from (SELECT * FROM [big-query-1233:bi.ga_downstream_table]) as a left join 
(SELECT a_orderId,a_offerId,
a_days_since_last_purchase,
a_txn,
 g_promo_code1,
 g_promo_length,
 g_promo_logic,
 g_promo_english,
 g_promo_desc,
 g_promo_type,
 g_flat_discount,
 g_percent_discount,
 g_max_cap_on_promo,
 g_CB_percent,
 g_CB_amount,
 g_is_cb,
 g_minimum_amount_se_valid,
 vouchers_redeemed
 
 FROM [big-query-1233:BI_marketing.promo_table] 
group by  a_orderId,a_offerId,
a_days_since_last_purchase,
 g_promo_code1,
 g_promo_length,
 g_promo_logic,
 g_promo_english,
 g_promo_desc,
 g_promo_type,
 g_flat_discount,
 g_percent_discount,
 g_max_cap_on_promo,
 g_CB_percent,
 g_CB_amount,
 g_is_cb,
 g_minimum_amount_se_valid,
 vouchers_redeemed,
 a_txn
 ) as b
on a.r_orderId =b.a_orderId and a.r_offerId = b.a_offerId"
##echo -e "Query: \n $v_query_downstream_promo";

tableName=downstream_promo
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_downstream_promo\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_downstream_promo" &
v_first_pid=$!
v_aag_Tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_downstream_promo_Tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_downstream_promo_Tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo " Downstream promo table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


