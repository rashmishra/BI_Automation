v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=dbdev;
date

v_query_deals_dtr_live_table="select  dealId, m.merchant_name, m.deal_owner,deal_end_date, category,

skusproductId,skusname, date(date) as Date,redeemvalidityfromdate,redeemvaliditytodate
from
(SELECT
  y.date AS date,
  y.Deal_ID AS dealId,
  'na' AS counter_value,
  m.merchant_name,
  m.deal_owner,
  y.Deal_End_Date AS deal_end_date,
  m.deal_category AS category,
  me.skusproductId as skusproductId, me.skusname as skusname,
 n.redeemvalidityfromdate as redeemvalidityfromdate, n.redeemvaliditytodate as redeemvaliditytodate
FROM
  [bi.live_deals_history_since_15_sep] y
INNER JOIN
  bi.deal_to_merchant m
ON
  m.deal_Id = y.Deal_ID
  
  inner join (select string(merchantid) as merchantid,skus.name as skusname,skus.productId as skusproductId from Atom.merchant) me on me.merchantid = m.merchantid
  
  inner join (select _id as dealid, DATE(MSEC_TO_TIMESTAMP(offers.offerValidity.redeemDates.fromDate)) as redeemvalidityfromdate, 
      DATE(MSEC_TO_TIMESTAMP(offers.offerValidity.redeemDates.toDate)) as redeemvaliditytodate 
      from flatten(flatten([big-query-1233:Atom.nile],offers.offerValidity.redeemDates.fromDate),offers.offerValidity.redeemDates.toDate)) n on n.dealid = y.Deal_ID
GROUP BY
  date,
  dealId,
  counter_value,
  deal_end_date,
  m.merchant_name,
  m.deal_owner,
  category,skusproductId, skusname,redeemvalidityfromdate,redeemvaliditytodate
  )
  
  group by 1,2,3,4,5,6,7,8,9,10"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=deals_dtr_live_table
v_destination_tbl="dbdev.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_deals_dtr_live_table\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_deals_dtr_live_table" &
v_first_pid=$!
v_liv_deals_pids+=" $v_first_pid"
wait $v_first_pid;

if wait $v_liv_deals_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_liv_deals_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in table loads" ;
fi

echo "deals dtr v/s BAU live table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com 


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0

