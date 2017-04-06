

#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

MONTH_NAME=$(date +%b)
echo $MONTH_NAME

echo $1
echo $2

bq --format=csv query --n=10000000 "SELECT
  oh.orderid AS orderid,
  SUM(unitprice/100) AS TransactionValue,
  DATE(MSEC_TO_TIMESTAMP(oh.createdat+19800000)) AS createdat,
  ol.dealid AS dealid,
  oh.source AS source,
  CASE 
  WHEN t.paymentmode =0 THEN 'Partner' 
  WHEN t.paymentmode =1 THEN 'PayU' 
  WHEN t.paymentmode =2 THEN 'PayTM' 
  WHEN t.paymentmode =3 THEN 'Mobi'
  WHEN t.paymentmode =4 THEN 'Citrus net'
  WHEN t.paymentmode =5 THEN 'Citrus CC/DC'
  WHEN t.paymentmode =6 THEN 'Freecharge'
ELSE 'NULL'  
END PaymentMode,
  CASE WHEN t.transactiontype=1 THEN 'credit' when t.transactiontype= 2 then 'cash' ELSE 'NULL' END transactiontype,
--  exact_count_distinct(oh.orderid) as Unique_Orders,
  Case when t.transactiontype= 2 then SUM(unitprice/100) end cash_transaction_value
FROM
  Atom.order_header AS oh
LEFT OUTER JOIN
  Atom.order_line AS ol
ON
  ol.orderid = oh.orderid
LEFT JOIN
  Atom.transaction AS T
ON
  T.orderid = oh.orderid
WHERE
  oh.ispaid = 't'
  AND month(MSEC_TO_TIMESTAMP(oh.createdat+19800000)) = 04
  AND year(MSEC_TO_TIMESTAMP(oh.createdat+19800000)) = 2017
  AND source IN ('web',
    'mobile-web')
GROUP BY
  orderid,
  createdat,
  dealid,
  transactiontype,
  t.paymentmode,
  paymentmode,
  source,
  t.transactiontype
  
order by createdat desc
"> /home/ubuntu/BI/data/google_drive/Daily_Web_Transactions/Apr/$1.csv

exit 0
