v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date


# appsflyer_processed_table loading. Replace existing
v_query_appsflyer_processed_table="select a.*,rank() over (partition by a.order_id_new order by ap.install_time asc) rank1, 
case when y.count > 1 then 'Reattributed' else 'na' end as reaatributed_flag from
(SELECT
  COALESCE(ap.order_id,ol.orderid,ap.af_receipt_id,ap.af_content_id) AS order_id_new,
  ap.* from
(SELECT
  imei,
  customer_user_id,
  attribution_type,
  media_source,
  appsflyer_device_id,
  fb_adgroup_id,
  download_time,
  fb_campaign_id, 
  is_retargeting,
  re_targeting_conversion_type,
  android_id,
  fb_adset_name,
  campaign,
  install_time,
  platform,
  fb_campaign_name,
  app_version,
  os_version,
  fb_adset_id,
  event_type,
  af_ad,
  af_ad_id,
  af_ad_type,
  af_adset,
  af_adset_id,
  af_c_id,
  af_channel,
  INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"order id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) order_id, 
  INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"transaction id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) transaction_id
  ,
  INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_content_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_content_id 
  ,INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_receipt_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_receipt_id
  FROM appsflyer.apps_flyer where 
  event_name in ('af_purchase','transactions - success','transactions','af_purchase_zero') 
  Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
  ) ap
  LEFT JOIN
  Atom.transaction AS ol
ON
  ap.transaction_id = ol.transactionid
  )a

left join 
(select order_id_new, count(*) as count
from (SELECT
  COALESCE(p.order_id,ol.orderid,p.af_receipt_id,p.af_content_id) AS order_id_new,
  p.* from
(SELECT
  INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"order id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) order_id, 
  INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"transaction id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) transaction_id,
  INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_content_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_content_id
   ,INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_receipt_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_receipt_id
  FROM appsflyer.apps_flyer where 
  event_name in ('af_purchase','transactions - success','transactions','af_purchase_zero') 
  ) p
  LEFT JOIN
  Atom.transaction AS ol
ON
  p.transaction_id = ol.transactionid
  )
  group by 1
  )y
  
  on a.order_id_new = y.order_id_new"
##echo -e "Query: n $v_query_CM_table_yesterday";

tableName=appslyer_processed_table_3
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_appsflyer_processed_table\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_appsflyer_processed_table" &
v_first_pid=$!
v_Apps_tbl_pids+=" $v_first_pid"
wait $v_first_pid;

# appslyer_processed_table_2 loading. Replace existing

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

echo "Appsflyer table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ## alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


