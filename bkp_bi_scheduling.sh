v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date



# scheduling_redemption loading. Replace existing
v_query_redemption="select oh.orderid, ol.orderlineID,  ol.offerId, ol.OfferTitle, ol.DealId, ol.Title, ol.VoucherId, ol.VoucherCode, oh.promocode, ol.CDA, (MSEC_TO_TIMESTAMP(ol.createdat+19800000)) as purchaseDate,(MSEC_TO_TIMESTAMP(ol.redemptionDate+19800000)) as redemptionDate,(Ol.redemptiondate-ol.createdat)/1000 as redemption_in_seconds,Voucherstatus, CustomerName, MerchantName, Unitprice/ 100 as OfferPrice,MarginPercentage, flatcommission/100, ol.redemptionbyrole as redemption_type, case when oh.orderid > 1991486 then ol.categoryid else e.category END as categoryid from Atom.order_header oh join Atom.order_line  ol on oh.orderid = ol.orderid left outer join (select string(_id)  as deal_id, dCat.id as category from Atom.deal) as e on ol.dealid = e.deal_id where ol.redemptionDate is not NULL"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=redemption_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_redemption\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_redemption" &
v_first_pid=$!
v_BI_sdl_pids+=" $v_first_pid"

wait $v_first_pid;


# refund_table loading. Replace existing
v_query_refund="SELECT
  tab1.olid AS order_line_id,
  tab1.oid AS refunded_orderid,
  tab1.up AS refunded_GB,
  tab1.up - tab1.fp AS reversed_promo,
  tab1.mp AS margin_percent,
  tab1.fc/100 AS flat_commission,
  YEAR(a.createdAt) AS purchased_year,
  MONTH(a.createdAt) AS purchased_month,
  DAY(a.createdAt) AS purchased_day,
  a.deal_owner,
  CASE WHEN tab1.mp IS NULL THEN ((CASE WHEN tab1.fc < 0 THEN 0 ELSE tab1.fc END)/100)*1 ELSE ((CASE WHEN tab1.mp < 0 THEN 0 ELSE tab1.mp END) / 100.0)*(tab1.up) END AS GR,
  CASE WHEN tab1.refunded_at IS NOT NULL THEN YEAR(MSEC_TO_TIMESTAMP(tab1.refunded_at+19800000)) ELSE YEAR(MSEC_TO_TIMESTAMP(tab1.udat+19800000)) END AS refunded_year,
  CASE WHEN tab1.refunded_at IS NOT NULL THEN MONTH(MSEC_TO_TIMESTAMP(tab1.refunded_at+19800000)) ELSE MONTH(MSEC_TO_TIMESTAMP(tab1.udat+19800000)) END AS refunded_Month,
  CASE WHEN tab1.refunded_at IS NOT NULL THEN DAY(MSEC_TO_TIMESTAMP(tab1.refunded_at+19800000)) ELSE DAY(MSEC_TO_TIMESTAMP(tab1.udat+19800000)) END AS refunded_Day,
  a.merchantname,
  a.dealid,
  a.categoryid,
  a.m_manager,
  tab1.ot AS offer_title,
  tab1.oi AS offer_id
FROM (
  SELECT
    merchantname,
    dealid,
    categoryid,
    m_location,
    m_manager,
    deal_owner,
    orderId,
    createdAt
  FROM
    BI_Automation.temp_CM_Table
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8) AS a
INNER JOIN (
  SELECT
    orderlineid AS olid,
    orderid AS oid,
    unitprice/100 AS up,
    finalprice/100 AS fp,
    marginpercentage AS mp,
    flatcommission AS fc,
    updatedat AS udat,
    closedAt AS refunded_at,
    offertitle AS ot,
    offerid AS oi
  FROM
    Atom.order_line
  WHERE
    status=16
    AND isPaid ='t' ) tab1
ON
  tab1.oid=a.orderId"
##echo -e "Query: \n $v_query_refund_table";

tableName=refund_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_refund\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_refund" &
v_first_pid=$!
v_BI_sdl_pids+=" $!"

wait $v_first_pid;

# cancellation_table loading. Replace existing
v_query_cancellation="SELECT
  YEAR(b.createdAt) AS purchase_year,
  MONTH(b.createdAt) AS purchase_month,
  DAY(b.createdAt) AS purchase_day,
  tab1.olid AS order_line_id,
  b.orderId AS order_cancelled,
  b.dealid,
  tab1.oi AS offer_id,
  b.categoryid,
  b.merchantname,
  tab1.ot AS offer_title,
  (tab1.up/100) AS cancelled_GB,
  (tab1.up - tab1.fp)/100 AS promo_reversal,
  b.promoCode AS cancelled_promo,
  tab1.fc/100 AS flat_commission,
  tab1.mp/100 AS margin_percentage,
  CASE WHEN tab1.mp IS NULL THEN ((CASE WHEN tab1.fc < 0 THEN 0 ELSE tab1.fc END)/100)*1 ELSE ((CASE WHEN tab1.mp < 0 THEN 0 ELSE tab1.mp END) / 100.0)*(tab1.up/100.0) END AS GR,
  YEAR(MSEC_TO_TIMESTAMP(tab1.cat+19800000)) AS cancellation_year,
  MONTH((MSEC_TO_TIMESTAMP(tab1.cat+19800000))) AS cancellation_month,
  DAY(MSEC_TO_TIMESTAMP(tab1.cat+19800000)) AS cancellation_day,
  b.m_manager AS manager,
  b.deal_owner AS deal_Owner
FROM (
  SELECT
    createdAt,
    orderId,
    dealid,
    categoryid,
    merchantname,
    promoCode,
    deal_owner,
    m_manager,
    m_location
  FROM
    BI_Automation.temp_CM_Table
  GROUP BY
    createdAt,
    orderId,
    dealid,
    categoryid,
    merchantname,
    promoCode,
    deal_owner,
    m_manager,
    m_location) AS b
INNER JOIN (
  SELECT
    orderlineid AS olid,
    unitprice AS up,
    orderid AS oid,
    cancelledAt AS cat,
    flatcommission AS fc,
    marginpercentage AS mp,
    finalprice AS fp,
    offertitle AS ot,
    offerid AS oi,
    MIN(bookingdate) AS bd
  FROM
    Atom.order_line
  WHERE
    status=17
    AND isPaid ='t'
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9) tab1
ON
  tab1.oid=b.orderId"
##echo -e "Query: \n $v_query_cancellation_table";

tableName=cancellation
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_cancellation\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_cancellation" &
v_first_pid=$!
v_BI_sdl_pids+=" $!"

wait $v_first_pid;


if wait $v_BI_sdl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"

if wait $v_BI_sdl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Table refresh status of refund table, cancellation table and redemption table in BI_Automation dataset:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com

exit 0
