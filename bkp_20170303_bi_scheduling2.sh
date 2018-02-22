v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=bi;
date



# scheduling_redemption loading. Replace existing
v_query_redemption="select oh.orderid, ol.orderlineID,  ol.offerId, ol.OfferTitle, ol.DealId
       , ol.Title, ol.VoucherId, ol.VoucherCode, oh.promocode, ol.CDA
       , (MSEC_TO_TIMESTAMP(ol.createdat+19800000)) as purchaseDate
       ,(MSEC_TO_TIMESTAMP(ol.redemptiondate+19800000)) as redemptiondate, (Ol.redemptiondate-ol.createdat)/1000 as redemption_in_seconds
       ,Voucherstatus, CustomerName, MerchantName, Unitprice/ 100 as OfferPrice
       , MarginPercentage, flatcommission/100, ol.redemptionbyrole as redemption_type
       , case when oh.orderid > 1991486 then ol.categoryid else e.category END as categoryid 
FROM Atom.order_header oh 
INNER JOIN Atom.order_line  ol on oh.orderid = ol.orderid 
LEFT OUTER JOIN (SELECT string(_id)  as deal_id, dCat.id as category 
                 FROM Atom.deal) as e on ol.dealid = e.deal_id 
WHERE ol.redemptiondate is not NULL"
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
  tab1.oi AS offer_id,
  e.comments as customer_comment
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
  tab1.oid=a.orderId
  inner join  Atom.transaction t on t.orderid = tab1.oid
    inner join Atom.event e on t.eventid = e.eventid"
##echo -e "Query: \n $v_query_refund_table";

tableName=refunds_table_old
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_refund\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_refund" &
v_first_pid=$!
v_BI_sdl_pids+=" $!"

wait $v_first_pid;



# New refund_table loading. Replace existing
v_query_refund_new="SELECT YEAR(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_year
      , MONTH(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_month
      , DAY(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_day
      , ol.olid AS order_line_id
      , ol.oid as refunded_orderid
      , ol.dealid as a_dealid
      , ol.oi AS offer_id
      , COALESCE(b.categoryid, ol.cat_id) AS a_categoryid
      , COALESCE(b.merchantname, ol.merchantname) as a_merchantname
      , ol.ot AS offer_title
      , (ol.up/100) AS refunded_GB
      , (ol.up - ol.fp)/100 AS promo_reversal
      , b.promoCode AS reversed_promo
      , ol.fc/100 AS flat_commission
      , ol.mp/100 AS margin_percent
      , CASE WHEN ol.mp IS NULL THEN ((CASE WHEN ol.fc < 0 THEN 0 ELSE ol.fc END)/100)*1 
             ELSE ((CASE WHEN ol.mp < 0 THEN 0 ELSE ol.mp END) / 100.0)*(ol.up/100.0) END AS GR
      , YEAR(MSEC_TO_TIMESTAMP(COALESCE(ol.cat, refunded_at) +19800000)) AS refunded_Year
      , MONTH((MSEC_TO_TIMESTAMP(COALESCE(ol.cat, refunded_at)+19800000))) AS refunded_Month
      , DAY(MSEC_TO_TIMESTAMP(COALESCE(ol.cat, refunded_at)+19800000)) AS refunded_Day
      , b.m_manager AS a_m_manager
      , b.deal_owner AS a_deal_owner
      , e.Customer_comments as customer_comment
      , e.Cancellation_source AS refunded_source
FROM (SELECT orderlineid AS olid, orderid AS oid
             , createdat, updatedat AS refunded_at, closedat AS cat 
             , unitprice AS up, flatcommission AS fc
             , marginpercentage AS mp, finalprice AS fp
             , dealid, offerid AS oi, deal.cat_id AS cat_id
             , offertitle AS ot, ol.merchantname AS merchantname
             , ol.merchantid AS merchantid, MIN(bookingdate) AS bd
        FROM Atom.order_line ol
        INNER JOIN (SELECT STRING(_id) AS Deal_ID, categoryId AS cat_id 
                    FROM Atom.deal GROUP BY 1,2) deal ON deal.Deal_ID = ol.dealid
        WHERE status=16
          AND isPaid ='t'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
             ) ol
LEFT JOIN  (SELECT t.orderid AS txn_orderid 
                   , ole.eventid AS eventid
                   , t.eventid AS txn_eventid
                   , ole.orderlineid AS orderlineid
                   , ole.Customer_comments AS Customer_comments   
                   , ole.cancellation_source AS Cancellation_source
            FROM (SELECT orderid, status, COALESCE(eventid, -1) as eventid
                         , transactiontype, updatedat, paymentflag
                         , paymentgatewayreference, failurereason, txnreason, paymentmode 
                  FROM Atom.transaction
                  WHERE status = 23
                    AND paymentflag <> 2
                  ) t
            LEFT JOIN (SELECT ole.eventid AS eventid
                             , ole.orderlineid as orderlineid
                             , ev.comments AS Customer_comments
                             , ev.source AS cancellation_source
                      FROM  [Atom.order_line_event] ole
                      INNER JOIN Atom.event ev ON ole.eventid = ev.eventid
                      GROUP BY 1, 2, 3, 4
                      )ole ON t.eventid = ole.eventid 
            GROUP BY 1, 2, 3, 4, 5, 6
            ) e ON ol.olid = e.orderlineid
               AND ol.oid = e.txn_orderid
LEFT JOIN (SELECT createdAt, orderId, dealid, categoryid
                   , merchantname, promoCode, deal_owner, m_manager
                   , m_location
            FROM BI_Automation.temp_CM_Table
            GROUP BY createdAt, orderId, dealid, categoryid
                   , merchantname, promoCode, deal_owner, m_manager
                   , m_location
                   ) AS b  ON ol.oid=b.orderId
GROUP BY purchased_year, purchased_month, purchased_day, order_line_id, refunded_orderid
        , a_dealid, offer_id, a_categoryid, a_merchantname, offer_title, refunded_GB
        , promo_reversal, reversed_promo, flat_commission, margin_percent, GR, refunded_Year
        , refunded_Month, refunded_Day, a_m_manager, a_deal_owner, customer_comment, refunded_source"
##echo -e "Query: \n $v_query_refund_table_new";

tableName=refunds_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_refund_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_refund_new" &
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
  b.deal_owner AS deal_Owner,
  e.comments as customer_comment
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
  tab1.oid=b.orderId
  inner join  Atom.transaction t on t.orderid = tab1.oid
    inner join Atom.event e on t.eventid = e.eventid"
##echo -e "Query: \n $v_query_cancellation_table";

tableName=Cancellation_old
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_cancellation\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_cancellation" &
v_first_pid=$!
v_BI_sdl_pids+=" $!"

wait $v_first_pid;



# New cancellation_table loading. Replace existing
v_query_cancellation_new="SELECT YEAR(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_year
      , MONTH(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_month
      , DAY(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_day
      , ol.olid AS order_line_id
      , ol.oid as order_cancelled
      , ol.dealid as b_dealid
      , ol.oi AS offer_id
      , COALESCE(b.categoryid, ol.cat_id) AS b_categoryid
      , COALESCE(b.merchantname, ol.merchantname) as b_merchantname
      , ol.ot AS offer_title
      , (ol.up/100) AS cancelled_GB
      , (ol.up - ol.fp)/100 AS promo_reversal
      , b.promoCode AS cancelled_promo
      , ol.fc/100 AS flat_commission
      , ol.mp/100 AS margin_percentage
      , CASE WHEN ol.mp IS NULL THEN ((CASE WHEN ol.fc < 0 THEN 0 ELSE ol.fc END)/100)*1 
             ELSE ((CASE WHEN ol.mp < 0 THEN 0 ELSE ol.mp END) / 100.0)*(ol.up/100.0) END AS GR
      , YEAR(MSEC_TO_TIMESTAMP(ol.cat+19800000)) AS cancellation_year
      , MONTH((MSEC_TO_TIMESTAMP(ol.cat+19800000))) AS cancellation_month
      , DAY(MSEC_TO_TIMESTAMP(ol.cat+19800000)) AS cancellation_day
      , b.m_manager AS manager
      , b.deal_owner AS deal_Owner
      , e.Customer_comments as customer_comment
      , e.Cancellation_source AS cancellation_source
FROM (SELECT orderlineid AS olid, unitprice AS up
             , createdat, orderid AS oid, cancelledAt AS cat
             , flatcommission AS fc, marginpercentage AS mp
             , finalprice AS fp, offertitle AS ot, dealid
             , offerid AS oi, deal.cat_id AS cat_id
             , ol.merchantname AS merchantname, ol.merchantid AS merchantid
             , MIN(bookingdate) AS bd
                  FROM Atom.order_line ol
                  INNER JOIN (SELECT STRING(_id) AS Deal_ID, categoryId AS cat_id 
                              FROM Atom.deal GROUP BY 1,2) deal ON deal.Deal_ID = ol.dealid
                  WHERE status=17
                    AND isPaid ='t'
                  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
             ) ol
LEFT JOIN  (SELECT t.orderid AS txn_orderid 
                   , ole.eventid AS eventid
                   , t.eventid AS txn_eventid
                   , ole.orderlineid AS orderlineid
                   , ole.Customer_comments AS Customer_comments   
                   , ole.cancellation_source AS Cancellation_source
            FROM (SELECT orderid, status, COALESCE(eventid, -1) as eventid
                         , transactiontype, updatedat, paymentflag
                         , paymentgatewayreference, failurereason, txnreason, paymentmode 
                  FROM Atom.transaction
                  WHERE status = 23
                    AND paymentflag <> 2
                  ) t
            LEFT JOIN (SELECT ole.eventid AS eventid
                             , ole.orderlineid as orderlineid
                             , ev.comments AS Customer_comments
                             , ev.source AS cancellation_source
                      FROM  [Atom.order_line_event] ole
                      INNER JOIN Atom.event ev ON ole.eventid = ev.eventid
                      GROUP BY 1, 2, 3, 4
                      )ole ON t.eventid = ole.eventid 
            GROUP BY 1, 2, 3, 4, 5, 6
            ) e ON ol.olid = e.orderlineid
               AND ol.oid = e.txn_orderid
LEFT JOIN (SELECT createdAt, orderId, dealid, categoryid
                   , merchantname, promoCode, deal_owner, m_manager
                   , m_location
            FROM BI_Automation.temp_CM_Table
            GROUP BY createdAt, orderId, dealid, categoryid
                   , merchantname, promoCode, deal_owner, m_manager
                   , m_location
                   ) AS b  ON ol.oid=b.orderId
GROUP BY purchase_year, purchase_month, purchase_day, order_line_id, order_cancelled
        , b_dealid, offer_id, b_categoryid, b_merchantname, offer_title, cancelled_GB
        , promo_reversal, cancelled_promo, flat_commission, margin_percentage, GR
        , cancellation_year, cancellation_month, cancellation_day, manager, deal_Owner, customer_comment
        , cancellation_source"
##echo -e "Query: \n $v_query_cancellation_table_new";

tableName=Cancellation
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_cancellation_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_cancellation_new" &
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

echo "Table refresh status of refund table, cancellation table and redemption table in BI dataset:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com alka.gupta@nearbuy.com

exit 0

