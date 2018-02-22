v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date

v_query_live="select
Date,
Merchant_ID,
Redemption_Town,
Business_Town,
deal_type,
Deal_ID, Category ,
merchant_type
from


(
select
Date,
Merchant_ID,
Redemption_Town,
Business_Town,
merchant_type
from
(
SELECT DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY')) AS Date
      , merchantId AS Merchant_ID
      , MSEC_TO_TIMESTAMP(createdAt + 19800000) AS Merchant_creation
      , MSEC_TO_TIMESTAMP(lastUpdateAt + 19800000) AS Merchant_last_update
      , IF( isDuplicate OR COALESCE(isDeListed, fALSE) OR COALESCE(isDeleted, FALSE)
               OR COALESCE(isTemporaryClosed, FALSE) OR COALESCE(isPermanentlyClosed, FALSE)
             , 'Closed Outlet', 'Live Outlet' ) AS Outlet_live_status
      , IF( isDuplicate OR COALESCE(isDeListed, fALSE) OR COALESCE(isDeleted, FALSE)
               OR COALESCE(isTemporaryClosed, FALSE) OR COALESCE(isPermanentlyClosed, FALSE)
               , FALSE, TRUE) AS Outlet_live
      , isActive AS is_Merchant_Active
      , status AS Merchant_Status
      , chainMerchantId 
      , redemptionAddress.cityTown AS Redemption_Town
      , businessAddress.cityTown AS Business_Town
      , case when merchantType = 'ONL' then 'Online' 
      when chainMerchantId is not null  then 'chain'
      else 'normal' end as merchant_type
FROM Atom.merchant
WHERE isPublished = TRUE
) 

where Outlet_live_status = 'Live Outlet' and Outlet_live is true and is_Merchant_Active is true

group by 1,2,3,4,5
)
a inner join

(

select  Deal_ID, Category , 

case when isPal is true then 'go' else 'prepaid' end as deal_type,
c.MID as MID

from [BI_Automation.live_deals_history_since_15_sep] a
inner join [Atom.deal] b on a.deal_id = b._id
inner join (select integer(mappings.merchant.id) as MID, integer(id) as id 
from flatten([Atom.mapping],mappings.merchant.id) where type = 'deal' group by 1,2) c on c.id = a.deal_id
where date(a.date) =  DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY'))
group by 1,2,3,4
) b on b.MID = a.merchant_id

group by 1,2,3,4,5,6,7,8
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=live_merchant_history_21feb2018
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_live\""
bq query --maximum_billing_tier 10000 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_live" &
v_first_pid=$!
v_merchant_pids+=" $v_first_pid"
wait $v_first_pid;



v_query_offline="select 
DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY')) as Date,
Merchant_ID , 
Redemption_Town ,   
    CATEGORY, 
    deal_type ,
    merchant_type
     from [BI_Automation.live_merchant_history_21feb2018] a
where date(date ) = DATE(DATE_ADD(CURRENT_DATE(), -2, 'DAY')) 

and merchant_id not in 
(
select
Merchant_ID
from [BI_Automation.live_merchant_history_21feb2018] 
where  date(date) = DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY'))
)
group by 1,2,3,4,5,6
"

tableName=offline_merchants_since_21feb2018
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_offline\""
bq query --maximum_billing_tier 10000 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_offline" &
v_first_pid=$!
v_merchant_pids+=" $v_first_pid"
wait $v_first_pid;




#  Merchant Made Live table loading. Append existing
v_query_madelive="select 
DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY')) as Date,
Merchant_ID , 
Redemption_Town ,   
    CATEGORY, 
    deal_type ,
    merchant_type
     from [BI_Automation.live_merchant_history_21feb2018] a
where date(date ) = DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY')) 

and merchant_id not in 
(
select
Merchant_ID
from [BI_Automation.live_merchant_history_21feb2018] 
where  date(date) = DATE(DATE_ADD(CURRENT_DATE(), -2, 'DAY'))
)
group by 1,2,3,4,5,6
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=merchant_madelive_since21feb2018
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_madelive\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_madelive" &
v_first_pid=$!
v_merchant_pids+=" $v_first_pid"
wait $v_first_pid;



# reengagement loading. Replace existing
v_query_madelive_firsttime=" select  
 DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY')) as Date,
 Merchant_ID , 
 --Deal_ID ,
 Category  , 
 deal_type,
 merchant_type,
 Redemption_Town
 from [BI_Automation.live_merchant_history_21feb2018]  
where date(Date) = DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY'))
and merchant_id not in 
(
select  Merchant_ID   from [nb_reports.outlets_open_with_deals] 
where date(reporting_date ) < DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY'))
and Outlet_live_status = 'Live Outlet'
)
group by 1,2,3,4,5,6"

tableName=merchant_live_first_time_21feb2018
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_madelive_firsttime\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_madelive_firsttime" &
##v_first_pid=$!
v_merchant_pids+=" $!"


if wait $v_merchant_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_merchant_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Merchant Live, offline, made live and made live first time Tables status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ## sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0
