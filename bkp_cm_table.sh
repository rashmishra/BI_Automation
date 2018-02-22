v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date


# CM_table_yesterday loading. Replace existing
v_query_temp_CM_Table="SELECT
  oh.orderid AS orderId,
  ol.vertical AS vertIcal,
  oh.customerid AS userId,
  (MSEC_TO_TIMESTAMP(oh.createdat+19800000)) AS createdAt,
  (SUM(ol.unitprice)/100.0) AS TransactionValue,
  (SUM(ol.finalprice) / 100.0) AS priceAfterPromo,
  oh.promocode AS promoCode,
  (oh.creditsRequested/100.0) AS creditPoints,
  oh.status AS orderStatus,
  oh.SOURCE AS SOURCE,
  ol.cda AS cda,
  Ol.dealid AS dealid,
  ol.offerid AS offerId,
  CASE WHEN ol.orderid > 1991486 THEN ol.categoryid ELSE e.category END AS categoryid,
  ol.offertitle,
  ol.merchantName AS merchantName,
  ol.merchantId AS merchantId,
  T.transactionId AS transactionId,
  T.paymentFlag AS paymentFlag,
  T.paymentgatewayreference AS paymentGatewayReference,
  COUNT (ol.orderid) AS NumberofVouchers,
  CASE WHEN ol.offerid = w.offer_correct THEN (w.marginGR/100)*(SUM(ol.unitprice)/100.0) ELSE ( CASE WHEN ol.marginPercentage IS NULL THEN ((CASE WHEN ol.flatcommission < 0 THEN 0 ELSE ol.flatcommission END)/100)*COUNT (ol.orderid) ELSE ((CASE WHEN ol.marginPercentage < 0 THEN 0 ELSE ol.marginPercentage END) / 100.0)*(SUM(ol.unitprice)/100.0) END) END AS GR,
  (T.payableAmount/100) AS payable_by_PG,
  D.deal_owner AS deal_owner,
  m.location,
  m.manager,
  CASE WHEN p.fpoid = oh.orderid THEN 'New' else 'Old' end as isnew, p.LPD,p.FPD,   
  (w.activation_cost)*COUNT (ol.orderid) as activation_cost1 ,
  buyinglat, buyinglong, l.city as buyingcity, l.state as buyingstate, promo.promocode as Promo
  FROM  Atom.order_header AS oh 
  LEFT OUTER JOIN Atom.order_line AS ol ON  ol.orderid = oh.orderid 
  left outer join (select string(_id)  as deal_id, dCat.id as category from Atom.deal) as e on ol.dealid = e.deal_id 
  left outer join (select OFFER_KEY  as offer_correct, MARGIN as margin_correct, integer(margin_GR) as marginGR, integer(Activation_cost) as activation_cost from bi.buy_price2) as w on ol.offerid = w.offer_correct  
  LEFT OUTER JOIN (select orderid, transactionid, paymentflag, paymentgatewayreference, payableamount from Atom.transaction where transactiontype = 2  AND   status = 23 AND payableAmount > 0 AND paymentFlag = 2 ) as T ON  T.orderid = oh.orderid 
  LEFT OUTER JOIN  (select string(_id) as dealid, dealOwner as deal_owner from Atom.deal) D on D.dealid = ol.dealid 
  left outer join bi.sales_rep_mapping1 as m on m.sales_rep = D.deal_owner 
  left outer join (select customerId as email1,   orderSummary.firstPurchaseDetail.orderId as fpoid, (MSEC_TO_TIMESTAMP(orderSummary.firstPurchaseDetail.purchasedate*1000+19800000)) as FPD, (MSEC_TO_TIMESTAMP(orderSummary.lastPurchaseDetail.purchasedate*1000+19800000)) as LPD, orderSummary.lastPurchaseDetail.purchasedate as lpd1 from Atom.customer where orderSummary.firstPurchaseDetail.orderId is not null ) as p on p.email1 = oh.customerid 
  left join [big-query-1233:latitude_longitude.latitude_longitude_base] l on l.latitude= oh.buyinglat and oh.buyinglong=l.longitude
 left join [big-query-1233:Atom.promocode] promo on promo.promoCodeId = oh.promocode
   where   ol.isPaid = 't' 
 GROUP BY orderId, vertIcal, userId, oh.createdat,oh.promocode,ol.offerid,creditPoints,payable_by_PG, orderStatus,SOURCE,cda,dealid,offerId,categoryid,ol.offertitle,merchantName,createdAt,promoCode,merchantId,transactionId,paymentFlag, paymentGatewayReference,deal_owner,m.location,isnew,oh.creditsRequested,T.payableAmount,w.activation_cost,ol.marginPercentage, 
  OL.flatcommission,m.manager,w.offer_correct,w.marginGR,p.LPD, p.FPD,oh.userid,oh.orderid,  buyinglat, buyinglong, buyingcity, buyingstate, promo"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=temp_CM_Table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_temp_CM_Table\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_temp_CM_Table" &
v_first_pid=$!
v_CM_Tbl_pids+=" $v_first_pid"
wait $v_first_pid;

# daily_reports_table_CM loading. Replace existing
v_query_daily_reports_table_CM="SELECT
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
  buyingcity,buyingstate,Promo
FROM
  BI_Automation.temp_CM_Table
LEFT OUTER JOIN (
  SELECT
    orderId AS oid,
    COUNT( orderId) AS count_oid
  FROM
    BI_Automation.temp_CM_Table
  GROUP BY
    1) s
ON
  s.oid = orderId
where date(createdat)!= current_date()
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
  Promo"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=daily_reports_table_CM
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_daily_reports_table_CM\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_daily_reports_table_CM" &
##v_first_pid=$!
v_CM_Tbl_pids+=" $!"



v_query_bi_daily_reports_table_CM="SELECT
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
  buyingcity,buyingstate,Promo
FROM
  BI_Automation.temp_CM_Table
LEFT OUTER JOIN (
  SELECT
    orderId AS oid,
    COUNT( orderId) AS count_oid
  FROM
    BI_Automation.temp_CM_Table

  GROUP BY
    1) s
ON
  s.oid = orderId
where date(createdat)!= current_date()
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
  Promo"
##echo -e "Query: \n $v_query_CM_bi_daily_reports_table_CM";

tableName=daily_reports_table_CM
v_destination_tbl="bi.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_bi_daily_reports_table_CM\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_bi_daily_reports_table_CM" &
##v_first_pid=$!
v_CM_Tbl_pids+=" $!"

if wait $v_CM_Tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_CM_Tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "CM table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ## akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


