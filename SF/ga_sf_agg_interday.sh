v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=dbdev;
date


# CM_table_yesterday loading. Replace existing
v_query_ga_sf_agg_interday_table="SELECT --storefront_id, storefront_id_version,
  user_group_id,
  template_id,
  a.entity_id AS ENTITY_ID,
  DATE(b.date) AS date,
  collection_id, 
  bc.title as collectiontitle,
  occasion_id,
  platform_type,
  bo.occassionname as occassionname,
  CASE
    WHEN b.action_type = '2' THEN 1
    ELSE 0
  END Productdetailviews,
  CASE
    WHEN b.action_type = '3' THEN 1
    ELSE 0
  END Addproducttocart,
  CASE
    WHEN b.action_type = '4' THEN 1
    ELSE 0
  END Removeproductfromcart,
  CASE
    WHEN b.action_type = '5' THEN 1
    ELSE 0
  END Checkout,
  CASE
    WHEN b.action_type = '6' THEN 1
    ELSE 0
  END Completedpurchase,
  CASE
    WHEN b.isimpression = TRUE THEN 1
    ELSE 0
  END impression,
  CASE
    WHEN b.isclick = TRUE THEN 1
    ELSE 0
  END clicks,
  d.entity_section_type as entity_section_type,
  d.entity_display_name as entity_display_name,
  c.sf_id AS SF_ID,
  c.sortOrder AS SORTORDER,
  c.platform AS PLATFORM,
  c.version AS VERSION,
  b.productrevenue AS productrevenue
FROM (
  SELECT sf_id as sf_id
      , version as version
, sortorder
      , entity_id
      , Entity_Name
      , platform
FROM 
(SELECT STRING(id) AS sf_id, sortOrder AS sortorder,
  sectionType AS Entity_Name, platform, STRING(version) AS version   
FROM [big-query-1233:storefront.section_definitions]
GROUP BY 1, 2, 3, 4, 5 
) sf1
LEFT JOIN [big-query-1233:storefront.entity_definition] ef
      ON ef.entity_section_type = sf1.Entity_Name
    ) c
LEFT JOIN (
  SELECT
    sessionid,
    storefront_id,
    storefront_id_version,
    user_group_id,
    template_id,
    entity_id,
    collection_id, 
    occasion_id,
    platform_type
  FROM
    ([big-query-1233:ga_simplified.ga_hit_product_custom_dim_intraday])
    ) a
ON
  c.sf_id = a.storefront_id
  AND c.version = a.storefront_id_version
  and c.entity_id = a.entity_id
LEFT JOIN (
  SELECT
    isclick,
    isimpression,
    sessionid,
    productrevenue,
    action_type,
    date
  FROM
    ([big-query-1233:ga_simplified.ga_hit_product_intraday])
    )b
ON
  a.sessionid = b.sessionid
LEFT JOIN
  [big-query-1233:storefront.entity_definition] d
ON
  d.entity_id = a.entity_id
  left join [big-query-1233:Atom.bravo_collection] bc on bc.collectionId = a.collection_Id
  left join(select occasionId, status, occasionAttrib.name as occassionname from [big-query-1233:Atom.bravo_occasion]) bo on bo.occasionId = a.occasion_Id
WHERE
  SF_ID!='' "
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=ga_sf_agg_interday_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_sf_agg_interday_table\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_ga_sf_agg_interday_table" &
v_first_pid=$!
v_ga_sf_int_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_ga_sf_int_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_ga_sf_int_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "ga_sf_agg_interday_table table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com 


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


