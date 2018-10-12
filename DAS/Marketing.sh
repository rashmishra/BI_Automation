v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=Marketing;
date


# CM_table_yesterday loading. Replace existing
v_query_Deal_to_ddv="SELECT
  --a. MONTH AS Month,
  a.Date AS Date,
  a.Category AS Category,
  a.DD AS Deal_Detail,
  b.GB AS GB,
  b.GR AS GR,
  a.Buy_Now as Buy_Now,
  b.Transaction AS Transactions,
  b.number_of_vouchers AS Number_of_vouchers,
  b.cashback_amount as cashback_amount,
  a.platform as platform,
  --a.did as device ,
  a.deal_id as deal_id,
  b.merchant_name as merchant_name ,
    b.deal_owner as deal_owner ,
    b.business_head as business_head ,
    b.a_business_head as a_business_head ,
   -- b.deal_type as deal_type,
    b.location as location
  --a.merchant_id as merchant_id
FROM (
select * from
    (
  SELECT
    --MONTH(date) MONTH,
    DATE(date) Date,
    CASE
      WHEN hits.product.v2ProductCategory IN ('ACT',  'TTD',  'HNL') THEN 'TTD, ACT & HNL'
      WHEN hits.product.v2ProductCategory IN ('FNB') THEN 'FNB'
      WHEN hits.product.v2ProductCategory IN ('SNS',  'BNS',  'SNM') THEN 'SNS, BNS & SNM'
      WHEN hits.product.v2ProductCategory IN ('GTW') THEN 'GTW'
      WHEN hits.product.v2ProductCategory IN ('HNF',  'HEA',  'FIT') THEN 'HNF, HEA & FIT'
      WHEN hits.product.v2ProductCategory IN ('LOR',
      'LOS') THEN 'LOR + LOS'
      WHEN hits.product.v2ProductCategory IN ('MVE') THEN 'MVE'
      ELSE 'None'
    END Category,
    case when  hits.sourcePropertyInfo.sourcePropertyDisplayName = 'nearbuy android' then 'android'
    when hits.sourcePropertyInfo.sourcePropertyDisplayName = 'nearbuy ios' then 'ios'
     when hits.sourcePropertyInfo.sourcePropertyDisplayName = 'mweb' then 'mweb'
     when hits.sourcePropertyInfo.sourcePropertyDisplayName  = 'nearbuy web' then 'Web'
     end Platform,
    hits.product.productSKU as Deal_Id,
    --case when hits.product.customDimensions.index = 81 then hits.product.customDimensions.value end as merchant_id,

    SUM(CASE
        WHEN hits.eCommerceAction.action_type='2' THEN 1
        ELSE 0
      END ) DD,
        SUM(CASE
        WHEN hits.eCommerceAction.action_type='3' THEN 1
         ELSE 0
       END ) Buy_Now,

  FROM
    TABLE_DATE_RANGE([124486161.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(),-6,'Month')), TIMESTAMP (CURRENT_DATE()))
  GROUP BY
    1,
    2,
    3,4)
    )
    
    
    AS a
LEFT JOIN (
  SELECT
    --MONTH(date_time_ist) AS MONTH,
    DATE(date_time_ist) AS date,
    CASE
      WHEN Category_id IN ('ACT',  'TTD',  'HNL') THEN 'TTD, ACT & HNL'
      WHEN Category_id IN ('FNB') THEN 'FNB'
      WHEN Category_id IN ('SNS',  'BNS',  'SNM') THEN 'SNS, BNS & SNM'
      WHEN Category_id IN ('GTW') THEN 'GTW'
      WHEN Category_id IN ('HNF',  'HEA',  'FIT') THEN 'HNF, HEA & FIT'
      WHEN Category_id IN ('LOR',
      'LOS') THEN 'LOR + LOS'
      WHEN Category_id IN ('MVE') THEN 'MVE'
      ELSE 'None'
    END Category,
    deal_id as deal_id ,
    merchant_name as merchant_name ,
    deal_owner ,
    business_head ,
    a_business_head ,
    case when platform_type = 'app_android' then 'android'
    when platform_type = 'app_ios' then 'ios'
     when platform_type in ('sbi-mobile-web','partnerplatform','sbi-mobile-web','mobile','mobile-web','shopclues-mobile-web') then 'mweb'
     else 'Web'
     end Platform,
    --deal_type,
    cm_location  as location,
   -- merchant_id as merchant_id ,
    SUM(GB) AS GB,
    SUM(GR) AS GR,
    ifnull(SUM(cashback_amount),0) AS cashback_amount,
    EXACT_COUNT_DISTINCT(order_id) AS Transaction,
    SUM(number_of_vouchers) AS number_of_vouchers,
    
   
  FROM
    [big-query-1233:nb_reports.master_transaction] 
  WHERE
    GB > 0
    AND deal_id NOT IN ('14324')
    --and month(date_time_ist) >= month(DATE_ADD(CURRENT_DATE(),-6,'Month'))
  GROUP BY
    1,
    2,
    3,4,5,6,7,8,9) AS b
ON
  a.date =b.date
  and a.Deal_Id = b.Deal_Id
  AND a.Category = b.Category
  and a.platform = b.platform"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=Deal_to_ddv 
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_Deal_to_ddv\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_Deal_to_ddv" &
v_first_pid=$!
v_Deal_to_ddv_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_Deal_to_ddv_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"


if wait $v_Deal_to_ddv_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo " Deal_to_ddv Table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


