v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=DTR_2017;
date


# CM_table_yesterday loading. Replace existing
v_query_temp_cm_rt="select
Date,
BAID,
Merchant_id,
Merchant_name,
Deal_ID,
Deal_title,
Category,
Category_Name,
Deal_Start_Date,
Deal_End_Date,
Offer_ID,
Offer_Title,
offer_product_id,
a.madeLiveAt as madeLiveAt,
paymentterm_id,
Redemption_from_Date,
Redemption_To_Date,
redemption_city,
redemption_state,
DEAL_TYPE,
dealOwner,
c.business_head as business_head, 
c.a_business_head as a_business_head,
c.location as location,
d.DTR_Spoc_EmailID as DTR_Spoc_EmailID , 
VALID_FOR_25DEC,
VALID_FOR_24DEC,
VALID_FOR_31DEC,
VALID_FOR_1JAN

--d.DTR_Team_member_email as DTR_Team_member_email


from
(
SELECT 
 date(msec_to_timestamp(now()/1000-24*60*60*1000)) as date,
 merchant.businessAccountId as BAID,
 merchant._id as Merchant_id,
 merchant.name as Merchant_name,
 _id as Deal_ID, 
 dealTitle as Deal_title,
 category._id as Category,
      category.name as Category_Name, 
      DATE(MSEC_TO_TIMESTAMP(startDate)) as Deal_Start_Date,
      DATE(MSEC_TO_TIMESTAMP(endDate)) as Deal_End_Date,
      
    offers.key as Offer_ID, 
    offers.offerTitle as Offer_Title,
    offers.skus.sku.productId as offer_product_id,
       date(MSEC_TO_TIMESTAMP(madeLiveAt)) as madeLiveAt,
       paymentTerm._id as paymentterm_id,
      date(msec_to_timestamp(offers.offerValidity.redeemDates.fromDate)) as Redemption_from_Date,
      date( msec_to_timestamp(offers.offerValidity.redeemDates.toDate) ) as Redemption_To_Date,
      merchant.redemptionAddress.cityTown as redemption_city,
      merchant.redemptionAddress.state as redemption_state,
      case 
          WHEN ((date(msec_to_timestamp(offers.offerValidity.redeemDates.fromDate)) IN ('2017-12-24','2017-12-25','2017-12-31','2018-01-01') 
      OR date(msec_to_timestamp(offers.offerValidity.redeemDates.TODate)) IN ('2017-12-24','2017-12-25','2017-12-31','2018-01-01')) 
      OR offers.offerTitle LIKE 'XMAS2017 & NYE2018%' OR offers.offerTitle LIKE 'XMAS2017/NYE2018%') then 'XMAS+NYE' 
      
      when ((date(msec_to_timestamp(offers.offerValidity.redeemDates.fromDate)) between '2017-12-24' and '2017-12-25'
      OR date(msec_to_timestamp(offers.offerValidity.redeemDates.TODate)) between '2017-12-24' and '2017-12-25') OR 
      offers.offerTitle LIKE 'XMAS2017%' OR offers.offerTitle LIKE '%24 Dec%' OR offers.offerTitle LIKE '(Early Bird) 24 Dec%'
      OR offers.offerTitle LIKE '%25 Dec%' OR offers.offerTitle LIKE '(Early Bird) 25 Dec%' OR offers.offerTitle LIKE '%23 Dec%' ) then 'XMAS' 
      WHEN ((date(msec_to_timestamp(offers.offerValidity.redeemDates.fromDate)) between '2017-12-31' and '2018-01-01' 
      OR date(msec_to_timestamp(offers.offerValidity.redeemDates.TODate)) between '2017-12-31' and '2018-01-01')
      OR offers.offerTitle LIKE 'NYE2018%' OR offers.offerTitle LIKE '%31 Dec%' OR offers.offerTitle LIKE '(Early Bird) 31 Dec%'
      OR offers.offerTitle LIKE '%1 Jan%' OR offers.offerTitle LIKE '(Early Bird) 1 Jan%' OR offers.offerTitle LIKE '%30 Dec%' ) then 'NYE'
  
      END AS DEAL_TYPE,
      
      CASE WHEN  offers.offerTitle LIKE '%23 Dec%' OR offers.offerTitle LIKE '%25 Dec%'  THEN 'YES' ELSE 'NO' END AS VALID_FOR_25DEC,
       CASE WHEN offers.offerTitle LIKE '%24 Dec%' OR offers.offerTitle LIKE '%XMAS2017%' OR offers.offerTitle LIKE '%XMAS2017 & NYE2018%'  THEN 'YES' ELSE 'NO' END AS VALID_FOR_24DEC,
        CASE WHEN offers.offerTitle LIKE '%29 Dec%' OR offers.offerTitle LIKE '%31 Dec%' OR offers.offerTitle LIKE '%NYE2018%' OR offers.offerTitle LIKE '%XMAS2017 & NYE2018%' THEN 'YES' ELSE 'NO' END AS VALID_FOR_31DEC,
         CASE WHEN offers.offerTitle LIKE '%1 Jan%' THEN 'YES' ELSE 'NO' END AS VALID_FOR_1JAN
      
      
 FROM FLATTEN(FLATTEN(FLATTEN(FLATTEN(FLATTEN(FLATTEN([big-query-1233:Atom.nile] , merchant.skus), offers)
                  , offers.offerValidity.redeemDates)
                  , offers.skus._id)
                  , mappedCategories.showOnHomePage)
                  , offers.calender.remainingQuantity)
                  
                  where startDate < (now()/1000-24*60*60*1000) and  endDate > (now()/1000-24*60*60*1000)
                    AND offers.isActive = TRUE and isActive = TRUE
                    AND (offers.calender.remainingQuantity IS NULL OR  offers.calender.remainingQuantity > 0)
                    and (offers.skus.sku.productId in (100295) 
                    or offers.offerTitle like 'XMAS2017%' or offers.offerTitle like 'NYE2018%')
                    and merchant.isPublished is true
                    
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
 ) a
 left join [Atom.deal] b on a.Deal_ID = b._id
 left join [nb_reports.sales_rep_mapping] c on b.dealowner = c.sales_rep
 left join [DTR_2017.DTR_SPOC] d on c.location = d.City
 
 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
 ORDER BY Deal_ID
 
 
 
 "
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=Offers_live2 
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --Append -n 1 --destination_table=$v_destination_tbl \"$v_query_temp_cm_rt\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_temp_cm_rt" &
v_first_pid=$!
v_CM_Tbl_pids+=" $v_first_pid"
wait $v_first_pid;

if wait $v_CM_Tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_CM_Tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Offers live status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


