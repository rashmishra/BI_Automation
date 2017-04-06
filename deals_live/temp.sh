


v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_Automation;
date


# CM_table_yesterday loading. Replace existing
v_query_live_deals_history="SELECT 
 date(msec_to_timestamp(now()/1000-24*60*60*1000)) as date,
 _id as Deal_ID, category._id as Category,
      category.name as Category_Name, 
      startDate as Deal_Start_Date_epoch, endDate as Deal_End_Date_epoch,
      DATE(MSEC_TO_TIMESTAMP(startDate)) as Deal_Start_Date, DATE(MSEC_TO_TIMESTAMP(endDate)) as Deal_End_Date,
      merchant.isActive Is_Merchant_Active ,
    offers.key as Offer_ID, 
    offers.offerTitle as Offer_Title,
    offers.isActive as Is_Offer_Active, 
      isMysteryDeal, madeLiveAt
      
 FROM FLATTEN(FLATTEN(FLATTEN(FLATTEN(FLATTEN(FLATTEN([big-query-1233:Atom.nile] , merchant.skus), offers)
                  , offers.offerValidity.redeemDates)
                  , offers.skus._id)
                  , mappedCategories.showOnHomePage)
                  , offers.calender.remainingQuantity)
                  where startDate < (now()/1000-24*60*60*1000) and  endDate > (now()/1000-24*60*60*1000)
                    AND offers.isActive = TRUE and isActive = TRUE
                    AND (offers.calender.remainingQuantity IS NULL OR  offers.calender.remainingQuantity > 0)
                    
 GROUP BY Deal_ID, Category, Category_Name, Deal_Start_Date_epoch, Deal_End_Date_epoch, 
         Deal_Start_Date, Deal_End_Date, Is_Merchant_Active, 
         isMysteryDeal, madeLiveAt, Offer_ID, Offer_Title,Is_Offer_Active,date
 ORDER BY Deal_ID"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=live_deals_history_since_15_sep
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --append -n 1 --destination_table=$v_destination_tbl \"$v_query_live_deals_history\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --append -n 0 --destination_table=$v_destination_tbl "$v_query_live_deals_history" &
v_first_pid=$!
v_liv_deals_pids+=" $v_first_pid"
wait $v_first_pid;

# daily_reports_table_CM loading. Replace existing

if wait $v_liv_deals_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_liv_deals_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in table loads" ;
fi

##echo "live deals history status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

##exit 0



#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

while read line
do
cd /home/ubuntu/BI_automation/deals_live
./deals_live.sh $line 10
done < /home/ubuntu/BI_automation/deals_live/deals_live.csv
cd /home/ubuntu/BI/data/google_drive
grive

echo "live deals history status:$v_table_status`date`:Please follow the link: https://drive.google.com/drive/folders/0Bw76R706sm29X3UwOUZVX2JRS1k" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com

##echo "Guys,

##TO check deals live today, 

##Please follow the link: https://drive.google.com/drive/folders/0Bw76R706sm29X3UwOUZVX2JRS1k

##Thanks in advance" | mutt -s "Updated deal data till $(date)" -c rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com ankur.sarawagi@nearbuy.com  alka.gupta@nearbuy.com anish.sinha@nearbuy.com  
-y


exit 0



