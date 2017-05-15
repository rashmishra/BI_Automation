
#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

# v_month_name=`date +%b`;
v_month_name=`date -d " -1 day" +%b`;
v_month_number=`date +%m`;
v_querying_month=`date -d " -1 day" +%m`;
v_querying_year=`date -d " -1 day" +%Y`;
v_manager_name=$1;

v_grive_folder="/home/ubuntu/BI/data/google_drive/Daily_Web_Transactions"

echo "${v_manager_name} it the Daily_Web_Transactions";
echo "${v_month_name} is the Month Name, used in Folder's name";
echo "${v_querying_month} is the Month Number, used in the query";


if [ ! -d "${v_grive_folder}/${v_month_name}/" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  mkdir ${v_grive_folder}/${v_month_name}/ ;
  chmod 0777 ${v_grive_folder}/${v_month_name} ;
else echo `date` " Directory for the month ${v_month_name} exists.";
fi


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
  AND month(MSEC_TO_TIMESTAMP(oh.createdat+19800000)) = ${v_querying_month}
  AND year(MSEC_TO_TIMESTAMP(oh.createdat+19800000)) = ${v_querying_year}
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
  
order by createdat desc" > ${v_grive_folder}/${v_month_name}/${v_manager_name}.csv

exit 0

