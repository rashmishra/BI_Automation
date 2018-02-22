v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=dbdev;
date

v_query_live_dtr_deals="SELECT 
 a._id as Deal_ID, category._id as Category,
      category.name as Category_Name, 
    a.offers.key as Offer_ID, 
    offers.offerTitle as Offer_Title,
      offers.skus.sku.product._id as offerproductid,
      dealTitle,
      merchant._id as merchantid, 
      merchant.name as merchantname,
      merchant.redemptionAddress.addressLine1 as RedemptionAddress, 
      merchant.redemptionAddress.cityTown as redeemptioncity,
      offers.pricing.msp as OfferMSP,
      offers.commission.flatComm as Commission ,
      offers.units.comm.percent as commpercentage
      
      
      
 FROM FLATTEN(FLATTEN(FLATTEN(FLATTEN(FLATTEN(FLATTEN([big-query-1233:Atom.nile] , merchant.skus), offers)
                  , offers.offerValidity.redeemDates)
                  , offers.skus._id)
                  , mappedCategories.showOnHomePage)
                  , offers.calender.remainingQuantity) a
                  
                  left join flatten([big-query-1233:Atom.offer],offers.units.comm.percent) b on a.offers.key = b.offers.key
                  where startDate < (now()/1000-24*60*60*1000) and  endDate > (now()/1000-24*60*60*1000)
                    AND a.offers.isActive = TRUE and a.isActive = TRUE
                    AND (offers.calender.remainingQuantity IS NULL OR  offers.calender.remainingQuantity > 0)
                    
 and offers.skus.sku.product._id in (100295,100296,100293,100294) 
 
 GROUP BY Deal_ID, Category, Category_Name,
       
         Offer_ID, Offer_Title,
         offerproductid,dealTitle,merchantid,merchantname,RedemptionAddress,redeemptioncity,OfferMSP,Commission,commpercentage
 ORDER BY Deal_ID"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=live_dtr_deals
v_destination_tbl="dbdev.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_live_dtr_deals\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_live_dtr_deals" &
v_first_pid=$!
v_liv_deals_pids+=" $v_first_pid"
wait $v_first_pid;

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

echo "DTR Deals list table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com 


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0

