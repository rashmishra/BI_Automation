v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=bi;
date


# CM_table_yesterday loading. Replace existing
v_query_aag_inventory_table="SELECT
 livedeals.*,
 sales.*,
 merchant.*
  from(
  SELECT
    deal_id,
    of AS qty,
    Offer_ID,
    Category,
    Deal_End_Date,
  FROM (
    SELECT
      DATE(MSEC_TO_TIMESTAMP(NOW()/1000-24*60*60*1000)) AS date,
      _id AS Deal_ID,
      category._id AS Category,
      category.name AS Category_Name,
      startDate AS Deal_Start_Date_epoch,
      endDate AS Deal_End_Date_epoch,
      DATE(MSEC_TO_TIMESTAMP(startDate)) AS Deal_Start_Date,
      DATE(MSEC_TO_TIMESTAMP(endDate)) AS Deal_End_Date,
      merchant.isActive Is_Merchant_Active,
      STRING(offers.key) AS Offer_ID,
      offers.offerTitle AS Offer_Title,
      offers.isActive AS Is_Offer_Active,
      offers.remainingQuantity,
      isMysteryDeal,
      madeLiveAt,
      offers.remainingQuantity AS of
    FROM
      FLATTEN(FLATTEN(FLATTEN([big-query-1233:Atom.nile], merchant.skus), offers), offers.skus._id)
    WHERE
      startDate < (NOW()/1000-24*60*60*1000)
      AND endDate > (NOW()/1000-24*60*60*1000)
      AND offers.isActive = TRUE
      AND isActive = TRUE
    GROUP BY
      Deal_ID,
      Category,
      Category_Name,
      Deal_Start_Date_epoch,
      Deal_End_Date_epoch,
      Deal_Start_Date,
      Deal_End_Date,
      Is_Merchant_Active,
      isMysteryDeal,
      madeLiveAt,
      Offer_ID,
      Offer_Title,
      Is_Offer_Active,
      offers.remainingQuantity,
      of,
      date)
  WHERE
    date = STRFTIME_UTC_USEC(NOW()-86400000000, \"%Y-%m-%d\")
  GROUP BY
    1,
    2,
    3,
    4,
    5
  ORDER BY
    Deal_ID) AS livedeals
LEFT JOIN (
  SELECT
    deal_Id,
    merchantId,
    merchant_name,
    deal_owner
  FROM
    [big-query-1233:bi.deal_to_merchant]
  GROUP BY
    1,
    2,
    3,
    4) AS merchant
ON
  merchant.deal_Id =livedeals.deal_id
LEFT OUTER JOIN (
  SELECT
    sales_rep,
    manager
  FROM
    [big-query-1233:bi.sales_rep_mapping1]
  GROUP BY
    1,
    2) AS sales
ON
  merchant.deal_owner = sales.sales_rep"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=aag_inventory_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_aag_inventory_table\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_aag_inventory_table" &
v_first_pid=$!
v_aag_Tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_aag_Tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_aag_Tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo " Live deals and Inventory table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com  alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


