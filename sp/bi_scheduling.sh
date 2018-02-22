v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=bi;
date


# New refund_table loading. Replace existing
v_query_refund_new="SELECT YEAR(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_year
      , MONTH(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_month
      , DAY(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_day
      , ol.olid AS order_line_id
      , ol.oid as refunded_order
      , ol.dealid as Deal_id
      , ol.oi AS offer_id
      , COALESCE(b.categoryid, ol.cat_id) AS Category_ID
      , COALESCE(b.merchantname, ol.merchantname) as Merchant_Name
      , ol.ot AS offer_title
      , (ol.up/100) AS refunded_GB
      , (ol.up - ol.fp)/100 AS promo_reversal
      , b.promoCode AS reversed_promo
      , ol.fc/100 AS flat_commission
      , ol.mp/100 AS margin_percentage
      , CASE WHEN ol.mp IS NULL THEN ((CASE WHEN ol.fc < 0 THEN 0 ELSE ol.fc END)/100)*1 
             ELSE ((CASE WHEN ol.mp < 0 THEN 0 ELSE ol.mp END) / 100.0)*(ol.up/100.0) END AS GR
      , YEAR(MSEC_TO_TIMESTAMP(COALESCE(ol.cat, refunded_at) +19800000)) AS refunded_Year
      , MONTH((MSEC_TO_TIMESTAMP(COALESCE(ol.cat, refunded_at)+19800000))) AS refunded_Month
      , DAY(MSEC_TO_TIMESTAMP(COALESCE(ol.cat, refunded_at)+19800000)) AS refunded_Day
      --, b.m_manager AS Manager
     -- , b.deal_owner AS Deal_Owner
      , e.Customer_comments as Customer_comment
      , e.Cancellation_source AS refunded_source,
      dealowner,
      location as city_manager_location ,
      manager as city_manager
FROM (SELECT orderlineid AS olid, orderid AS oid
             , createdat, updatedat AS refunded_at, closedat AS cat 
             , unitprice AS up, flatcommission AS fc
             , marginpercentage AS mp, finalprice AS fp
             , dealid, offerid AS oi, deal.cat_id AS cat_id
             , offertitle AS ot, ol.merchantname AS merchantname
             , ol.merchantid AS merchantid,
             deal.dealowner as dealowner,
             sp.location as location , sp.manager as  manager,
             MIN(bookingdate) AS bd
        FROM Atom.order_line ol
        INNER JOIN (SELECT STRING(_id) AS Deal_ID, categoryId AS cat_id , dealowner
                    FROM Atom.deal GROUP BY 1,2,3) deal ON deal.Deal_ID = ol.dealid
                    left join BI_Automation.sales_rep_mapping sp on sp.sales_rep = deal.dealowner
        WHERE status=16
          AND isPaid ='t'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,17,18
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
            FROM bi.temp_daily_cm_dont_delete
            GROUP BY createdAt, orderId, dealid, categoryid
                   , merchantname, promoCode, deal_owner, m_manager
                   , m_location
                   ) AS b  ON ol.oid=b.orderId
                          AND b.orderId = e.txn_orderid
GROUP BY purchase_year, purchase_month, purchase_day, order_line_id, refunded_order, Deal_id
         , offer_id, Category_ID, Merchant_Name, offer_title, refunded_GB, promo_reversal
         , reversed_promo, flat_commission, margin_percentage, GR, refunded_Year, refunded_Month
         , refunded_Day, --Manager, Deal_Owner, 
         Customer_comment, refunded_source, city_manager_location, city_manager, dealowner"
##echo -e "Query: \n $v_query_refund_table_new";

tableName=refunds_table_new
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_refund_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_refund_new" &
v_first_pid=$!
v_BI_sdl_pids+=" $!"

wait $v_first_pid;


# New cancellation_table loading. Replace existing
v_query_cancellation_new="SELECT YEAR(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_year
      , MONTH(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_month
      , DAY(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_day
      , ol.olid AS order_line_id
      , ol.oid as order_cancelled
      , ol.dealid as deal_id
      , ol.oi AS offer_id
      , COALESCE(b.categoryid, ol.cat_id) AS Category_ID
      , COALESCE(b.merchantname, ol.merchantname) as Merchant_Name
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
      --, b.m_manager AS manager
      --, b.deal_owner AS deal_Owner
      , e.Customer_comments as customer_comment
      , e.Cancellation_source AS cancellation_source,
      dealowner,
       location as city_manager_location, manager as city_manager
FROM (SELECT orderlineid AS olid, unitprice AS up
             , createdat, orderid AS oid, cancelledAt AS cat
             , flatcommission AS fc, marginpercentage AS mp
             , finalprice AS fp, offertitle AS ot, dealid
             , offerid AS oi, deal.cat_id AS cat_id
             , ol.merchantname AS merchantname, ol.merchantid AS merchantid,
             deal.dealowner as dealowner,
             sp.location as location, sp.manager as manager
             , MIN(bookingdate) AS bd
                  FROM Atom.order_line ol
                  INNER JOIN (SELECT STRING(_id) AS Deal_ID, categoryId AS cat_id , dealowner
                              FROM Atom.deal GROUP BY 1,2,3) deal ON deal.Deal_ID = ol.dealid
                              left join BI_Automation.sales_rep_mapping sp on sp.sales_rep = deal.dealowner
                  WHERE status=17
                    AND isPaid ='t'
                  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,15,16,17
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
            FROM bi.daily_reports_table_cm_new
           
            GROUP BY createdAt, orderId, dealid, categoryid
                   , merchantname, promoCode, deal_owner, m_manager
                   , m_location 
                   ) AS b  ON ol.oid=b.orderId
                          AND b.orderId = e.txn_orderid
                         -- where  ol.dealid = '20562'
GROUP BY purchase_year, purchase_month, purchase_day, order_line_id, order_cancelled
        , deal_id, offer_id, Category_ID, Merchant_Name, offer_title, cancelled_GB
        , promo_reversal, cancelled_promo, flat_commission, margin_percentage, GR
        , cancellation_year, cancellation_month, cancellation_day,-- manager, deal_Owner, 
        customer_comment
        , cancellation_source,dealowner,city_manager_location,city_manager"
##echo -e "Query: \n $v_query_cancellation_table_new";

tableName=Cancellation_new
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

echo "Table refresh status of refund table and cancellation tablein BI dataset:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com alka.gupta@nearbuy.com

exit 0

