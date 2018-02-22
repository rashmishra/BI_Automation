v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=adhoc_requests;
date


# CM_table_yesterday loading. Replace existing
v_query_deals_live="SELECT  dd_deal_id as Deal_ID, dd_deal_title  as Deal_Title
       , dd_valid_from as valid_from
       , dd_made_live_at as Deal_Made_Live_At
       , dd_valid_till as valid_till
       , mapd_merchant_id as Merchant_ID
FROM [Atom_simplified.deal_detail] dd
INNER JOIN [Atom_simplified.mapping_deal] mapd
    ON mapd.mapd_deal_id = dd.dd_deal_ID
WHERE dd_deal_state  = 'live'
  AND dd_valid_till  IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=deals_live  
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_deals_live\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_deals_live" &
v_first_pid=$!
v_deals_live_pids+=" $v_first_pid"
wait $v_first_pid;

# daily_reports_table_CM loading. Replace existing
v_query_live_merchants_information="SELECT
  orderId,
  vertIcal,
  userId,
  createdAt,
  TransactionValue,
  priceAfterPromo,
  promoCode,
  SUM(creditPoints)/s.count_oid AS creditPoints,
  orderStatus,
  SOURCE,
  cda,
  dealid,
  offerId,
  categoryid,
  ol_offertitle,
  merchantName,
  merchantId,
  transactionId,
  paymentFlag,
  paymentGatewayReference,
  NumberofVouchers,
  GR,
  SUM(payable_by_PG)/s.count_oid AS payable_by_PG,
  deal_owner,
  m_location,
  m_manager,
  isnew,
  p_LPD,
  p_FPD,
  activation_cost1,
  1/s.count_oid AS txn,
  buyinglat, 
  buyinglong, 
  buyingcity,buyingstate,Promo,
 a.totalcashback as totalcashback
FROM
  BI_Automation.temp_cm_rt a
LEFT OUTER JOIN (
  SELECT
    orderId AS oid,
    COUNT(orderId) AS count_oid
  FROM
     BI_Automation.temp_cm_rt
  GROUP BY
    1) s
ON
  s.oid = a.orderId
  --where date(createdat)!= current_date()
GROUP BY
  orderId,
  vertIcal,
  userId,
  createdAt,
  TransactionValue,
  priceAfterPromo,
  promoCode,
  orderStatus,
  SOURCE,
  cda,
  dealid,
  offerId,
  categoryid,
  ol_offertitle,
  merchantName,
  merchantId,
  transactionId,
  paymentFlag,
  paymentGatewayReference,
  NumberofVouchers,
  GR,
  deal_owner,
  m_location,
  m_manager,
  isnew,
  p_LPD,
  p_FPD,
  activation_cost1,
  s.count_oid,
  txn,
  buyinglat, 
  buyinglong, 
  buyingcity,
  buyingstate,
  Promo,
  totalcashback"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=live_merchants_information
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_live_merchants_information\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_live_merchants_information" &
v_second_pid=$!
v_deals_live_pids+=" $!"

wait $v_second_pid;

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

echo "deals_live & Live merchant information Table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


