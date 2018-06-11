ta_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=das;
date


# incentive tableloading. Replace existing
v_query_incentive="Select * from
(SELECT
    string(Merchant_ID) MID,
    CategoryId Category,
    sales_rep SR,
    a_business_head ABH,
    Business_Head BH,
    City_Head CH,
    MIN(reporting_date) date
  FROM (
    SELECT
      Merchant_ID,
      Deal_ID,
      CategoryId,
      reporting_date,
      dealOwner
    FROM (
      SELECT
        Merchant_ID,
        Deal_ID,
        reporting_date
      FROM
        [big-query-1233:nb_reports.outlets_open_with_deals]
      WHERE
        Outlet_live_status='Live Outlet'
        AND reporting_date >='2018-05-01') a
    INNER JOIN (
      SELECT
        _id,
        categoryId,
        dealOwner
      FROM
        [big-query-1233:Atom.deal]
      WHERE
        CategoryId IN ('BNS',
          'SNM') )b
    ON
      a. Deal_ID=b. _id
    GROUP BY
      1,
      2,
      3,
      4,
      5) a
  INNER JOIN (
    SELECT
      Location,
      Business_Head,
      a_business_head,
      City_Head,
      sales_rep
    FROM
      [big-query-1233:nb_reports.sales_rep_mapping]
      where Role='SR') b
  ON
    a. dealOwner =b. sales_rep
  WHERE
    Merchant_id NOT IN (
    SELECT
      Merchant_ID
    FROM
      [big-query-1233:nb_reports.outlets_open_with_deals]
    WHERE
      Outlet_live_status='Live Outlet'
      AND DATE(reporting_date) BETWEEN DATE(DATE_ADD(CURRENT_TIMESTAMP(),-3,'month'))
      AND DATE(DATE_ADD(CURRENT_TIMESTAMP(),-1,'month')))
  GROUP BY
   1,
    2,
    3,
    4,
    5,
    6
    ),
    
    (SELECT
  mappings.merchant.id MID ,
  category Category,
  sales_rep SR,
  a_business_head ABH,
  Business_Head BH,
  City_Head CH,
  DATE(MSEC_TO_TIMESTAMP(updateHistory.submittedAt+19800000)) date from
  (select _id,
  category ,
  sales_rep ,
  a_business_head ,
  Business_Head ,
  City_Head ,
  updateHistory.submittedAt from
  (SELECT
    _id,
    categoryId,
    units.contractType,
    dealOwner,
    updateHistory.submittedAt
  FROM
    [big-query-1233:Atom.deal]
  WHERE
    units.contractType='PAL' and date(msec_to_timestamp(updateHistory.submittedAt+19800000))>='2018-05-01') a
INNER JOIN (
  SELECT
    sales_rep,
    City_Head,
    a_business_head,
    Business_Head,
    Location,
    Category
  FROM
    [big-query-1233:nb_reports.sales_rep_mapping] where Role='SR' ) b
ON
  a. dealOwner =b. sales_rep
GROUP BY 1,2,3,4,5,6,7) a
left join 
(select integer(id) d, mappings.merchant.id from [big-query-1233:Atom.mapping] where type ='deal') b
on a. _id=b. d
group by 
  1,
  2,
  3,
  4,
  5,
  6,
  7
  )"

  ##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=incentive
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_incentive\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_incentive" &
v_first_pid=$!
v_incent_pids+=" $v_first_pid"
wait $v_first_pid;


# incentive_tracker loading. Replace existing
v_query_incentive_tracker="select exact_count_distinct(MID), case when Category in('SNM','BNS') then 'BWH' else 'FNB' end Category, SR, ABH,BH,CH from
(select * from
(SELECT
    Deal_ID DID,
    string(Merchant_ID) MID,
    CategoryId Category,
    sales_rep SR,
    a_business_head ABH,
    Business_Head BH,
    City_Head CH,
    MIN(reporting_date) date
  FROM (
    SELECT
      Merchant_ID,
      Deal_ID,
      CategoryId,
      reporting_date,
      dealOwner
    FROM (
      SELECT
        Merchant_ID,
        Deal_ID,
        reporting_date
      FROM
        [big-query-1233:nb_reports.outlets_open_with_deals]
      WHERE
        Outlet_live_status='Live Outlet'
        AND reporting_date >='2018-05-01') a
    INNER JOIN (
      SELECT
        _id,
        categoryId,
        dealOwner
      FROM
        [big-query-1233:Atom.deal]
      WHERE
        CategoryId IN ('BNS',
          'SNM') )b
    ON
      a. Deal_ID=b. _id
    GROUP BY
      1,
      2,
      3,
      4,
      5) a
  INNER JOIN (
    SELECT
      Location,
      Business_Head,
      a_business_head,
      City_Head,
      sales_rep
    FROM
      [big-query-1233:nb_reports.sales_rep_mapping]
      where Role='SR') b
  ON
    a. dealOwner =b. sales_rep
  WHERE
    Merchant_id NOT IN (
    SELECT
      Merchant_ID
    FROM
      [big-query-1233:nb_reports.outlets_open_with_deals]
    WHERE
      Outlet_live_status='Live Outlet'
      AND DATE(reporting_date) BETWEEN DATE(DATE_ADD(CURRENT_TIMESTAMP(),-3,'month'))
      AND DATE(DATE_ADD(CURRENT_TIMESTAMP(),-1,'month')))
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
    ),
    
    (SELECT
  _id DID,
  mappings.merchant.id MID ,
  category Category,
  sales_rep SR,
  a_business_head ABH,
  Business_Head BH,
  City_Head CH,
  DATE(MSEC_TO_TIMESTAMP(updateHistory.submittedAt+19800000)) date from
  (select _id,
  category ,
  sales_rep ,
  a_business_head ,
  Business_Head ,
  City_Head ,
  updateHistory.submittedAt from
  (SELECT
    _id,
    categoryId,
    units.contractType,
    dealOwner,
    updateHistory.submittedAt
  FROM
    [big-query-1233:Atom.deal]
  WHERE
    units.contractType='PAL' and date(msec_to_timestamp(updateHistory.submittedAt+19800000))>='2018-05-01') a
INNER JOIN (
  SELECT
    sales_rep,
    City_Head,
    a_business_head,
    Business_Head,
    Location,
    Category
  FROM
    [big-query-1233:nb_reports.sales_rep_mapping] where Role='SR' ) b
ON
  a. dealOwner =b. sales_rep
GROUP BY 1,2,3,4,5,6,7) a
left join 
(select integer(id) d, mappings.merchant.id from [big-query-1233:Atom.mapping] where type ='deal') b
on a. _id=b. d
group by 
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8)) where date>='2018-06-01'
  group by 2,3,4,5,6"



  tableName=incentive_tracker
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_incentive_tracker\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_incentive_tracker" &
##v_first_pid=$!
v_incentt_pids+=" $!"


if wait $v_incentt_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_incentt_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo " Incentive and Incentive_Tracker Tables status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0

