v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=BI_marketing;
date


# CM_table_yesterday loading. Replace existing
v_query_promo_table="SELECT a.*,g.*, s.count1 as vouchers_redeemed from 
(select orderId, userId, createdAt, TransactionValue, priceAfterPromo, creditPoints, SOURCE, dealid, offerId, 
categoryid, 
ol_offertitle, merchantName, transactionId, merchantId, paymentFlag, paymentGatewayReference, 
NumberofVouchers, GR, payable_by_PG, deal_owner, m_location, m_manager, isnew, p_LPD, p_FPD, activation_cost1, 
buyinglat as oh_buyinglat, buyinglong as oh_buyinglong, buyingcity as user_location, 0 as days_since_last_purchase, txn , left(promoCode,12) as ignore1 
FROM [big-query-1233:BI_Automation.daily_reports_table_CM] )a 
left outer join  
(SELECT promoCodeId as promo_code1, length(lcPromoCodeId) as promo_length, lcTitle as promo_logic, promoCode as promo_english, lcDescription as promo_desc,  
promoCodeDiscount.promoCodeType as promo_type, 
promoCodeDiscount.discountAmount as flat_discount, promoCodeDiscount.discountPercent as percent_discount, 
promoCodeDiscount.maxCap as max_cap_on_promo, promoCodeDiscount.cashbackPercent as CB_percent, promoCodeDiscount.cashbackAmount as CB_amount,isCashback as is_cb, 
promoCodeDiscount.offerPriceRange.from as minimum_amount_se_valid, 
  FROM [big-query-1233:Atom.promocode] ) g 
on g.promo_code1 = a.ignore1 
left outer join 
(select oh_orderid as oid, ol_offerId as ofid, exact_count_distinct( ol_orderlineID) as count1 from BI_Automation.redemption_table group by 1,2) s 
on s.oid = a.orderId and s.ofid = a.offerId"
##echo -e "Query: \n $v_query_CM_table_yesterday";

tableName=promo_table
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_promo_table\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_promo_table" &
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

echo "promo table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


