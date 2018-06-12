v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=temp;
date


# query for the new table
v_query_appsflyer_processed_table="SELECT
  (Merchant_ID),
  categoryId,
  CASE
    WHEN city IN ('Pimpri Chinchwad', 'Pune') THEN 'Pune'
    WHEN city IN ('Mohali',
    'Panchkula',
    'Chandigarh',
    'Zirakpur') THEN 'Chandigarh'
    WHEN city IN ('Thane', 'Navi Mumbai', 'Mumbai', 'Vasai Virar') THEN 'Mumbai'
    WHEN city IN ('New Delhi',
    'Ghaziabad',
    'Faridabad') THEN 'New Delhi'
    WHEN city = 'Gurgaon' THEN 'Gurgaon'
    WHEN city IN ('Noida',
    'Greater Noida',
    'Hosur') THEN 'Noida'
    WHEN city = 'Bengaluru' THEN 'Bengaluru'
    WHEN city = 'Chennai' THEN 'Chennai'
    WHEN city IN ('Kolkata', 'Salt Lake City', 'New Town', 'Bidhan Nagar', 'Kalikapur', 'Barrackpore', 'Howrah') THEN 'Kolkata'
    WHEN city = 'Jaipur' THEN 'Jaipur'
    WHEN city IN ('Hyderabad', 'Secunderabad') THEN 'Hyderabad'
    WHEN city = 'Pune' THEN 'Pune'
    WHEN city = 'Ahmedabad' THEN 'Ahmedabad'
    ELSE 'Others'
  END AS city,
  Date
FROM (
  SELECT
    Merchant_ID,
    date,
    categoryId
  FROM (
    SELECT
      Merchant_ID,
      Deal_ID,
      Redemption_Town Town,
      DATE(reporting_date) date
    FROM
      [big-query-1233:nb_reports.outlets_open_with_deals]
    WHERE
      Outlet_live_status = 'Live Outlet'
    GROUP BY
      1,
      2,
      3,
      4)a
  INNER JOIN (
    SELECT
      (_id) _id,
      categoryId
    FROM
      [big-query-1233:Atom.deal]
    WHERE
      categoryId IN ('BNS',
        'SNM') )b
  ON
    a.Deal_ID = b._id)a
LEFT JOIN (
  SELECT
    redemptionAddress.cityTown city,
    name,
    (merchantId) merchantId
  FROM
    [big-query-1233:Atom.merchant]
  WHERE
    catInfo.key IN ('SNM',
      'BNS')
  GROUP BY
    1,
    2,
    3)b
ON
  a.Merchant_ID = b.merchantId
GROUP BY
  1,
  2,
  3,
  4"


tableName=temp_SY


v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_appsflyer_processed_table\""


bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_appsflyer_processed_table" &
v_first_pid=$!
v_Apps_tbl_pids +=" $v_first_pid"
wait $v_first_pid;

//check if all query ran Successfully 
if wait $v_Apps_tbl_pids; 
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_Apps_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Appsflyer table status:$v_table_status`date`" | mail -s "$v_table_status" sudarshan.yadav@nearbuy.com 


exit 0


