v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date

# ga_alldata_table loading. Append existing
v_query_ga_reengagementdata="SELECT  a.orderId AS orderId
  , a.vertIcal AS vertIcal
  , a.userId AS customerid
  , a.createdAt AS createdat
  , a.TransactionValue AS TransactionValue
  , a.priceAfterPromo AS priceAfterPromo
  , a.promoCode AS promoCode
  , a.creditPoints AS creditPoints
  , a.SOURCE AS SOURCE
  , a.dealid AS dealid
  , a.offerId AS offerId
  , a.categoryid AS categoryid
  , a.ol_offertitle AS Offertitle
  , a.merchantName AS merchantName
  , a.merchantId AS merchantId
  , a.transactionId AS transactionId
  , a.NumberofVouchers AS NumberofVouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.m_location AS location_name
  , a.isnew AS isnew
  , a.p_LPD AS Last_purchase_date
  , a.p_FPD AS First_purchase_date
  ,1 as rank1
  ,y.dcg_ga AS dcg_GA
  ,y.campaign_grouping AS campaign_grouping
  ,y.campaign_ga AS campaign_GA
  ,y.source_ga AS source_GA
  ,y.medium_ga AS medium_GA
  ,y.keyword_ga AS keyword_GA
  ,y.content_ga AS content_GA
  ,y.dealid_ga AS dealid_GA
  ,ap.a_ap_imei  as ap_ap_imei
  ,ap.a_ap_attribution_type as ap_ap_attribution_type
  ,ap.a_ap_media_source as ap_ap_media_source
  ,ap.a_ap_fb_adgroup_id as ap_ap_fb_adgroup_id
  ,STRING(ap.a_ap_download_time) as ap_ap_download_time
  ,ap.a_ap_fb_campaign_id as ap_ap_fb_campaign_id
  ,ap.a_ap_campaign  as ap_ap_campaign
  ,STRING(ap.a_ap_install_time)  as ap_ap_install_time
  ,ap.a_ap_platform as ap_ap_platform
  ,ap.a_ap_fb_campaign_name  as ap_ap_fb_campaign_name
  ,ap.a_ap_app_version as ap_ap_app_version
  ,ap.a_ap_os_version as ap_ap_os_version
  ,ap.a_ap_fb_adset_id as ap_ap_fb_adset_id
  ,ap.a_ap_event_type as ap_ap_event_type
  ,ap.a_ap_af_ad as ap_ap_af_ad
  ,ap.a_ap_af_ad_id as ap_ap_af_ad_id
  ,ap.a_ap_af_ad_type  as ap_ap_af_ad_type
  ,ap.a_ap_af_adset as ap_ap_af_adset
  ,ap.a_ap_af_adset_id as ap_ap_af_adset_id
  ,ap.a_ap_af_c_id as ap_ap_af_c_id
  ,ap.a_ap_af_channel as ap_ap_af_channel
  ,reaatributed_flag
  ,ap.a_ap_is_retargeting as is_retargeting,
 ap.a_ap_re_targeting_conversion_type as is_reengagement
  
 FROM 
 BI_Automation.CM_table_yesterday a
 LEFT JOIN  
 (select * from BI_Automation.appsflyer_reengagement_table2 where rank1 = 1) as ap on a.orderId = ap.a_order_id_new 
 LEFT JOIN (
  SELECT
    dcg AS dcg_ga
    ,campaign_grouping 
    ,campaign AS campaign_ga
    ,source AS source_ga
    ,medium AS medium_ga
    ,keyword AS keyword_ga
    ,content AS content_ga
    ,dealid AS dealid_ga
    ,INTEGER (orderid) AS orderid_ga
  FROM BI_Automation.ga_orderid_table_yesterday
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  ) AS y ON a.orderId = y.orderid_ga
 GROUP BY 1
  ,2
  ,3
  ,4
  ,7
  ,SOURCE
  ,cda
  ,dealid
  ,offerId
  ,categoryid
  ,merchantName
  ,merchantId
  ,transactionId
  ,deal_owner
  ,location_name
  ,isnew
  ,dcg_GA
  ,campaign_grouping
  ,campaign_GA
  ,source_GA
  ,medium_GA
  ,keyword_GA
  ,content_GA
  ,Offertitle
  ,dealid_GA
  ,Last_purchase_date
  ,First_purchase_date
  ,ap_ap_imei 
  ,ap_ap_attribution_type 
  ,ap_ap_media_source 
  ,ap_ap_fb_adgroup_id 
  ,ap_ap_download_time 
  ,ap_ap_fb_campaign_id 
  ,ap_ap_campaign 
  ,ap_ap_install_time 
  ,ap_ap_platform 
  ,ap_ap_fb_campaign_name 
  ,ap_ap_app_version 
  ,ap_ap_os_version 
  ,ap_ap_fb_adset_id 
  ,ap_ap_event_type 
  ,ap_ap_af_ad 
  ,ap_ap_af_ad_id 
  ,ap_ap_af_ad_type 
  ,ap_ap_af_adset 
  ,ap_ap_af_adset_id 
  ,ap_ap_af_c_id 
  ,ap_ap_af_channel 
  ,TransactionValue,priceAfterPromo,creditPoints,NumberofVouchers,GR,a.orderId,a.userId,reaatributed_flag  ,is_retargeting,
 is_reengagement"
##echo -e "Query: \n $v_query_ga_alldata_table";

tableName=ga_reengagementdata
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_reengagementdata\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_ga_reengagementdata" &
##v_first_pid=$!
v_downs_rpt_pids+=" $!"

wait $!;

# ga_alldata_table_new loading. Append existing
v_query_ga_reengagementdata_new="SELECT  a.orderId AS orderId
  , a.vertIcal AS vertIcal
  , a.userId AS customerid
  , a.createdAt AS createdat
  , a.TransactionValue AS TransactionValue
  , a.priceAfterPromo AS priceAfterPromo
  , a.promoCode AS promoCode
  , a.creditPoints AS creditPoints
  , a.SOURCE AS SOURCE
  , a.dealid AS dealid
  , a.offerId AS offerId
  , a.categoryid AS categoryid
  , a.ol_offertitle AS Offertitle
  , a.merchantName AS merchantName
  , a.merchantId AS merchantId
  , a.transactionId AS transactionId
  , a.NumberofVouchers AS NumberofVouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.m_location AS location_name
  , a.isnew AS isnew
  , a.p_LPD AS Last_purchase_date
  , a.p_FPD AS First_purchase_date
  ,1 as rank1
  ,y.dcg_ga AS dcg_GA
  ,y.campaign_grouping AS campaign_grouping
  ,y.campaign_ga AS campaign_GA
  ,y.source_ga AS source_GA
  ,y.medium_ga AS medium_GA
  ,y.keyword_ga AS keyword_GA
  ,y.content_ga AS content_GA
  ,y.dealid_ga AS dealid_GA
   ,ap.a_ap_imei  as ap_ap_imei
  ,ap.a_ap_attribution_type as ap_ap_attribution_type
  ,ap.a_ap_media_source as ap_ap_media_source
  ,ap.a_ap_fb_adgroup_id as ap_ap_fb_adgroup_id
  ,STRING(ap.a_ap_download_time) as ap_ap_download_time
  ,ap.a_ap_fb_campaign_id as ap_ap_fb_campaign_id
  ,ap.a_ap_campaign  as ap_ap_campaign
  ,STRING(ap.a_ap_install_time) as ap_ap_install_time
  ,ap.a_ap_platform as ap_ap_platform
  ,ap.a_ap_fb_campaign_name  as ap_ap_fb_campaign_name
  ,ap.a_ap_app_version as ap_ap_app_version
  ,ap.a_ap_os_version as ap_ap_os_version
  ,ap.a_ap_fb_adset_id as ap_ap_fb_adset_id
  ,ap.a_ap_event_type as ap_ap_event_type
  ,ap.a_ap_af_ad as ap_ap_af_ad
  ,ap.a_ap_af_ad_id as ap_ap_af_ad_id
  ,ap.a_ap_af_ad_type  as ap_ap_af_ad_type
  ,ap.a_ap_af_adset as ap_ap_af_adset
  ,ap.a_ap_af_adset_id as ap_ap_af_adset_id
  ,ap.a_ap_af_c_id as ap_ap_af_c_id
  ,ap.a_ap_af_channel as ap_ap_af_channel
  ,reaatributed_flag,
  ap.a_ap_is_retargeting as is_retargeting,
 ap.a_ap_re_targeting_conversion_type as is_reengagement
  
 FROM 
BI_Automation.CM_table_yesterday a
 LEFT JOIN  
 (select * from BI_Automation.appsflyer_reengagement_table2 where rank1 = 1) as ap on a.orderId = ap.a_order_id_new 
 LEFT JOIN (
  SELECT
    dcg AS dcg_ga
    ,campaign_grouping 
    ,campaign AS campaign_ga
    ,source AS source_ga
    ,medium AS medium_ga
    ,keyword AS keyword_ga
    ,content AS content_ga
    ,dealid AS dealid_ga
    ,INTEGER (orderid) AS orderid_ga
  FROM BI_Automation.ga_orderid_table_yesterday
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  ) AS y ON a.orderId = y.orderid_ga
 where a.isnew = 'New' 
 GROUP BY 1
  ,2
  ,3
  ,4
  ,7
  ,SOURCE
  ,cda
  ,dealid
  ,offerId
  ,categoryid
  ,merchantName
  ,merchantId
  ,transactionId
  ,deal_owner
  ,location_name
  ,isnew
  ,dcg_GA
  ,campaign_grouping
  ,campaign_GA
  ,source_GA
  ,medium_GA
  ,keyword_GA
  ,content_GA
  ,Offertitle
  ,dealid_GA
  ,Last_purchase_date
  ,First_purchase_date
  ,ap_ap_imei 
  ,ap_ap_attribution_type 
  ,ap_ap_media_source 
  ,ap_ap_fb_adgroup_id 
  ,ap_ap_download_time 
  ,ap_ap_fb_campaign_id 
  ,ap_ap_campaign 
  ,ap_ap_install_time 
  ,ap_ap_platform 
  ,ap_ap_fb_campaign_name 
  ,ap_ap_app_version 
  ,ap_ap_os_version 
  ,ap_ap_fb_adset_id 
  ,ap_ap_event_type 
  ,ap_ap_af_ad 
  ,ap_ap_af_ad_id 
  ,ap_ap_af_ad_type 
  ,ap_ap_af_adset 
  ,ap_ap_af_adset_id 
  ,ap_ap_af_c_id 
  ,ap_ap_af_channel 
  ,TransactionValue,priceAfterPromo,creditPoints,NumberofVouchers,GR,a.orderId,a.userId,reaatributed_flag,is_retargeting,
 is_reengagement"
##echo -e " Query: \n $v_query_ga_alldata_table_new";

tableName=ga_reengagementdata_new 
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_reengagementdata_new\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_ga_reengagementdata_new" &
v_downs_rpt_pids+=" $!"


wait $!;


# ga_alldata_table_yesterday loading. Replace existing
v_query_ga_reengagementdata_yesterday="SELECT  a.orderId AS orderId
  , a.vertIcal AS vertIcal
  , a.userId AS customerid
  , a.createdAt AS createdat
  , a.TransactionValue AS TransactionValue
  , a.priceAfterPromo AS priceAfterPromo
  , a.promoCode AS promoCode
  , a.creditPoints AS creditPoints
  , a.SOURCE AS SOURCE
  , a.dealid AS dealid
  , a.offerId AS offerId
  , a.categoryid AS categoryid
  , a.ol_offertitle AS Offertitle
  , a.merchantName AS merchantName
  , a.merchantId AS merchantId
  , a.transactionId AS transactionId
  , a.NumberofVouchers AS NumberofVouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.m_location AS location_name
  , a.isnew AS isnew
  , a.p_LPD AS Last_purchase_date
  , a.p_FPD AS First_purchase_date
  ,1 as rank1
  ,y.dcg_ga AS dcg_GA
  ,y.campaign_grouping AS campaign_grouping
  ,y.campaign_ga AS campaign_GA
  ,y.source_ga AS source_GA
  ,y.medium_ga AS medium_GA
  ,y.keyword_ga AS keyword_GA
  ,y.content_ga AS content_GA
  ,y.dealid_ga AS dealid_GA
   ,ap.a_ap_imei  as ap_ap_imei
  ,ap.a_ap_attribution_type as ap_ap_attribution_type
  ,ap.a_ap_media_source as ap_ap_media_source
  ,ap.a_ap_fb_adgroup_id as ap_ap_fb_adgroup_id
  ,STRING(ap.a_ap_download_time) as ap_ap_download_time
  ,ap.a_ap_fb_campaign_id as ap_ap_fb_campaign_id
  ,ap.a_ap_campaign  as ap_ap_campaign
  ,STRING(ap.a_ap_install_time)  as ap_ap_install_time
  ,ap.a_ap_platform as ap_ap_platform
  ,ap.a_ap_fb_campaign_name  as ap_ap_fb_campaign_name
  ,ap.a_ap_app_version as ap_ap_app_version
  ,ap.a_ap_os_version as ap_ap_os_version
  ,ap.a_ap_fb_adset_id as ap_ap_fb_adset_id
  ,ap.a_ap_event_type as ap_ap_event_type
  ,ap.a_ap_af_ad as ap_ap_af_ad
  ,ap.a_ap_af_ad_id as ap_ap_af_ad_id
  ,ap.a_ap_af_ad_type  as ap_ap_af_ad_type
  ,ap.a_ap_af_adset as ap_ap_af_adset
  ,ap.a_ap_af_adset_id as ap_ap_af_adset_id
  ,ap.a_ap_af_c_id as ap_ap_af_c_id
  ,ap.a_ap_af_channel as ap_ap_af_channel
  ,reaatributed_flag
    ,ap.a_ap_is_retargeting as is_retargeting,
 ap.a_ap_re_targeting_conversion_type as is_reengagement
  
 FROM 
 BI_Automation.CM_table_yesterday a
 LEFT JOIN  
 (select * from BI_Automation.appsflyer_reengagement_table2 where rank1 = 1) as ap on a.orderId = ap.a_order_id_new 
 LEFT JOIN (
  SELECT
    dcg AS dcg_ga
    ,campaign_grouping 
    ,campaign AS campaign_ga
    ,source AS source_ga
    ,medium AS medium_ga
    ,keyword AS keyword_ga
    ,content AS content_ga
    ,dealid AS dealid_ga
    ,INTEGER (orderid) AS orderid_ga
  FROM BI_Automation.ga_orderid_table_yesterday
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  ) AS y ON a.orderId = y.orderid_ga
 GROUP BY 1
  ,2
  ,3
  ,4
  ,7
  ,SOURCE
  ,cda
  ,dealid
  ,offerId
  ,categoryid
  ,merchantName
  ,merchantId
  ,transactionId
  ,deal_owner
  ,location_name
  ,isnew
  ,dcg_GA
  ,campaign_grouping
  ,campaign_GA
  ,source_GA
  ,medium_GA
  ,keyword_GA
  ,content_GA
  ,Offertitle
  ,dealid_GA
  ,Last_purchase_date
  ,First_purchase_date
  ,ap_ap_imei 
  ,ap_ap_attribution_type 
  ,ap_ap_media_source 
  ,ap_ap_fb_adgroup_id 
  ,ap_ap_download_time 
  ,ap_ap_fb_campaign_id 
  ,ap_ap_campaign 
  ,ap_ap_install_time 
  ,ap_ap_platform 
  ,ap_ap_fb_campaign_name 
  ,ap_ap_app_version 
  ,ap_ap_os_version 
  ,ap_ap_fb_adset_id 
  ,ap_ap_event_type 
  ,ap_ap_af_ad 
  ,ap_ap_af_ad_id 
  ,ap_ap_af_ad_type 
  ,ap_ap_af_adset 
  ,ap_ap_af_adset_id 
  ,ap_ap_af_c_id 
  ,ap_ap_af_channel
  ,TransactionValue,priceAfterPromo,creditPoints,NumberofVouchers,GR,a.orderId,a.userId,reaatributed_flag  , is_retargeting,
is_reengagement
"
##echo -e "Query: \n $v_query_ga_alldata_table_yesterday";

tableName=ga_reengagementdata_yesterday
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_reengagementdata_yesterday\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_ga_reengagementdata_yesterday" &
v_downs_rpt_pids+=" $!"


wait $!;

# ga_downstream_table loading. Replace existing
v_query_ga_reengagement_table="SELECT 
  r.*, 
  v.* 
FROM 
(select 
orderId as orderId,
vertIcal as vertIcal,
customerid as customerid,
createdat as createdat,
TransactionValue as TransactionValue,
priceAfterPromo as priceAfterPromo,
promoCode as promoCode,
creditPoints as creditPoints,
SOURCE as SOURCE,
dealid as dealid,
offerId as offerId,
categoryid as categoryid,
Offertitle as Offertitle,
merchantName as merchantName,
merchantId as merchantId,
transactionId as transactionId,
NumberofVouchers as NumberofVouchers,
GR as GR,
deal_owner as deal_owner,
location_name as location_name,
isnew as isnew,
Last_purchase_date as Last_purchase_date,
First_purchase_date as First_purchase_date,
rank1 as rank1,
dcg_GA as dcg_GA,
campaign_grouping as campaign_grouping,
campaign_GA as campaign_GA,
source_GA as source_GA,
medium_GA as medium_GA,
keyword_GA as keyword_GA,
content_GA as content_GA,
dealid_GA as dealid_GA,
ap_ap_imei as ap_ap_imei
  ,ap_ap_attribution_type as ap_ap_attribution_type
  ,ap_ap_media_source as ap_ap_media_source
  ,ap_ap_fb_adgroup_id as ap_ap_fb_adgroup_id
  ,ap_ap_download_time as ap_ap_download_time
  ,ap_ap_fb_campaign_id as ap_ap_fb_campaign_id
  ,ap_ap_campaign as ap_ap_campaign
  ,ap_ap_install_time as ap_ap_install_time
  ,ap_ap_platform as ap_ap_platform
  ,ap_ap_fb_campaign_name as ap_ap_fb_campaign_name
  ,ap_ap_app_version as ap_ap_app_version
  ,ap_ap_os_version as ap_ap_os_version
  ,ap_ap_fb_adset_id as ap_ap_fb_adset_id
  ,ap_ap_event_type as ap_ap_event_type
  ,ap_ap_af_ad as ap_ap_af_ad
  ,ap_ap_af_ad_id as ap_ap_af_ad_id
  ,ap_ap_af_ad_type as ap_ap_af_ad_type
  ,ap_ap_af_adset as ap_ap_af_adset
  ,ap_ap_af_adset_id as ap_ap_af_adset_id
  ,ap_ap_af_c_id as ap_ap_af_c_id
  ,ap_ap_af_channel as ap_ap_af_channel
  ,reaatributed_flag
,is_retargeting,
is_reengagement
from 
  BI_Automation.ga_reengagementdata_yesterday )
r 
LEFT OUTER JOIN (
  SELECT
    dealid AS first_dealid,
    categoryid AS first_category,
    promoCode AS first_promo,
    dcg_ga AS first_DCG,
    campaign_grouping as first_campaign_grouping,
    source_ga AS first_source,
    medium_ga AS first_medium,
    campaign_ga AS first_campaign,
    keyword_ga AS first_keyword,
    content_ga AS first_content,
    merchantName AS first_merchant,
    location_name AS first_location,
    customerid AS first_customerid,
    SOURCE AS firstSource,
    createdat as fpd,
    ap_ap_attribution_type as first_app_attribution,
    ap_ap_media_source as first_app_media_source,
    ap_ap_campaign as first_app_campaign, 
    ap_ap_platform as first_app_platform,
   ap_ap_app_version as first_appv,
    ap_ap_os_version as first_osv ,
    is_retargeting as first_is_retargeting, is_reengagement as first_is_reengagement  
  FROM
    BI_Automation.ga_reengagementdata_new 
  group by first_dealid,first_app_attribution, first_category,fpd,first_appv,  first_app_media_source, first_app_campaign, first_app_platform,first_appv,  first_osv , first_promo, first_DCG, first_campaign_grouping, first_source, first_medium, first_campaign, first_keyword,  first_content, first_merchant, first_location, first_customerid, firstSource,first_is_retargeting, first_is_reengagement ) AS v ON 
  r.customerid = v.first_customerid"
##echo -e "Query: \n $v_query_ga_downstream_table";

tableName=ga_reengagement_table
v_destination_tbl="bi.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_reengagement_table\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_ga_reengagement_table" &
v_downs_rpt_pids+=" $!"


wait $!;

# ga_downstream_table loading. Replace existing
v_query_ga_reengagement_table2="SELECT a.*, b.offerid  FROM [big-query-1233:bi.ga_reengagement_table] a
left join (select  string(Deal_Id) as offerid from [big-query-1233:dbdev.live_dtr_deals]) b on a.r_dealid = b.offerid"


tableName=ga_reengagement_table2
v_destination_tbl="bi.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_reengagement_table2\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_ga_reengagement_table2" &
v_downs_rpt_pids+=" $!"



## End of CNR1


if wait $v_downs_rpt_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"

if wait $v_downs_rpt_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Re-engagement table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com alka.gupta@nearbuy.com

exit 0
 
