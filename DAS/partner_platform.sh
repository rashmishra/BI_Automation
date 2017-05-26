v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=DAS;
date


# CM_table_yesterday loading. Replace existing
v_query_partner_platform="select

*, 
 case when date(mt.date_time_ist)  BETWEEN  date(Date_of_Registration) and  date(DATE_ADD(Date_of_Registration, 30, 'DAY')) and Registration_source = 'PARTNER_PLATFORM' then 'PP_IN_30'
 when Registration_source != 'PARTNER_PLATFORM' then 'NP' else 'PP_out_30'
 end customer_type,
 case when date(mt.date_time_ist)  BETWEEN  date([Date_of_Registration]) and  date(DATE_ADD([Date_of_Registration], 30, 'DAY')) then 'within_30day_txn_date' ELSE 'NULL' end within_30day_txn_date

from 
(
select 
mt.*,
c.Date_of_Registration as Date_of_Registration,
c.Registration_source as Registration_source,
DATEDIFF(date(mt.first_purchase_date ),date(c.Date_of_Registration)) as date_diff,
case when    COALESCE(c.partnerid, oh.partnerid) contains 'x1lLGIjqdR8dUSjqQA22C4fxv9yexq62I0wBE1pi' then 'Helpchat'
  when COALESCE(c.partnerid, oh.partnerid) contains 'EGZicvz68YyiYR9PKsYS4zayzQswpAf6SAhlKKJb'then 'MySmartPrice'
  when COALESCE(c.partnerid, oh.partnerid) contains 'dFHE046zqimzEpaez6aC3iGMR7BpePTRxH6rQ620' then 'AdvantageClub'
  when COALESCE(c.partnerid, oh.partnerid) contains 'gO4YUdmwEy2leBKdNgPFm85ytkUhrUo0awy28VkU' then 'HDFC'
  END AS Partner_Type,


from
(select mt.*,
DATE_ADD((CONCAT(STRING(YEAR(DATE_ADD(((first_purchase_date )),1, 'Month'))),'-',STRING(MONTH(DATE_ADD(((first_purchase_date )),1, 'Month'))),'-1')), -1, 'DAY') as last_day_of_FP_month 
from nb_reports.master_transaction mt
) mt
left join Atom.order_header oh on oh.orderid = mt.order_id
left join (
select 
case when created_date2 like '1970%' then created_date else created_date2 end Date_of_Registration ,
customerid,
partnerid,
Registration_source
from
(
select 
customerid, 
date(msec_to_timestamp(createdAt)) as created_date,
date(sec_to_timestamp(createdAt)) as created_date2 ,
partnerid,
sourceCreatedBy as Registration_source
from Atom.customer 
)
)
c on c.customerid = mt.customer_id

)"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=partner_platform
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_partner_platform\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_partner_platform" &
v_first_pid=$!
v_pp_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_pp_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_pp_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo " Partner Platform table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com 


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


