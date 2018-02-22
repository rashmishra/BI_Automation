#!/bin/bash

v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date



# scheduling_redemption loading. Replace existing
v_query_redemption="SELECT oh.orderid, ol.orderlineID,  ol.offerId, ol.OfferTitle, ol.DealId, ol.Title
        , ol.VoucherId, ol.VoucherCode, oh.promocode, ol.CDA
        , (MSEC_TO_TIMESTAMP(ol.createdat+19800000)) as purchaseDate,(MSEC_TO_TIMESTAMP(ol.redemptionDate+19800000)) as redemptionDate
        ,(Ol.redemptiondate-ol.createdat)/1000 as redemption_in_seconds,Voucherstatus
        , CustomerName, MerchantName, Unitprice/ 100 as OfferPrice,MarginPercentage, flatcommission/100
        , ol.redemptionbyrole as redemption_type
        , case when oh.orderid > 1991486 then ol.categoryid else e.category END as categoryid 
FROM Atom.order_header oh 
INNER JOIN Atom.order_line  ol ON oh.orderid = ol.orderid 
LEFT OUTER JOIN (SELECT string(_id)  as deal_id, dCat.id as category 
                 FROM Atom.deal) as e on ol.dealid = e.deal_id 
WHERE ol.redemptionDate IS NOT NULL"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=redemption_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_redemption\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_redemption" &
v_first_pid=$!
v_BI_sdl_pids+=" $v_first_pid"



wait $v_first_pid;


# New refund_table loading. Replace existing
v_query_refund_new="SELECT YEAR(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_year
      , MONTH(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_month
      , DAY(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_day
      , ol.olid AS order_line_id
      , ol.oid as refunded_orderid
      , ol.dealid as a_dealid
      , ol.oi AS offer_id
      , COALESCE(b.category_id, ol.cat_id) AS a_categoryid
      , COALESCE(b.merchant_name, ol.merchantname) as a_merchantname
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
      , b.city_manager AS a_m_manager
      , b.deal_owner AS a_deal_owner
      , e.Customer_comments as Customer_comment
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
LEFT JOIN (SELECT date_time_ist , order_Id, deal_id, category_id
                   , merchant_name, promoCode, deal_owner, city_manager
                   , cm_location
            FROM nb_reports.master_transaction 
            GROUP BY date_time_ist , order_Id, deal_id, category_id
                   , merchant_name, promoCode, deal_owner, city_manager
                   , cm_location
                   ) AS b  ON ol.oid=b.order_Id
GROUP BY purchased_year, purchased_month, purchased_day, order_line_id, refunded_orderid, a_dealid
         , offer_id, a_categoryid, a_merchantname, offer_title, refunded_GB, promo_reversal
         , reversed_promo, flat_commission, margin_percent, GR, refunded_Year, refunded_Month
         , refunded_Day, a_m_manager, a_deal_owner, Customer_comment, refunded_source";
##echo -e "Query: \n $v_query_refund_table";

tableName=refund_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_refund_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_refund_new" &
v_first_pid=$!
v_BI_sdl_pids+=" $!"

wait $v_first_pid;

# cancellation_table loading. Replace existing
v_query_cancellation_new="SELECT YEAR(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_year
      , MONTH(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_month
      , DAY(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchase_day
      , ol.olid AS order_line_id
      , ol.oid as order_cancelled
      , ol.dealid as b_dealid
      , ol.oi AS offer_id
      , COALESCE(b.category_id, ol.cat_id) AS b_categoryid
      , COALESCE(b.merchant_name, ol.merchantname) as b_merchantname
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
      , b.city_manager AS manager
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
LEFT JOIN (SELECT date_time_ist , order_Id, deal_id, category_id
                   , merchant_name, promoCode, deal_owner, city_manager
                   , cm_location
            FROM nb_reports.master_transaction
            GROUP BY date_time_ist , order_Id, deal_id, category_id
                   , merchant_name, promoCode, deal_owner, city_manager
                   , cm_location
                   ) AS b  ON ol.oid=b.order_Id
GROUP BY purchase_year, purchase_month, purchase_day, order_line_id, order_cancelled
        , b_dealid, offer_id, b_categoryid, b_merchantname, offer_title, cancelled_GB
        , promo_reversal, cancelled_promo, flat_commission, margin_percentage, GR
        , cancellation_year, cancellation_month, cancellation_day, manager, deal_Owner, customer_comment
        , cancellation_source"
##echo -e "Query: \n $v_query_cancellation_table";

tableName=cancellation
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

echo "Table refresh status of refund table, cancellation table and redemption table in BI_Automation dataset: $v_table_status `date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com alka.gupta@nearbuy.com

exit 0

