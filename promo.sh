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
buyinglat as oh_buyinglat, buyinglong as oh_buyinglong, buyingcity as user_location, 0 as days_since_last_purchase, txn , left(promoCode,12) as Promo_code 
FROM [big-query-1233:BI_Automation.daily_reports_table_CM] )a 
left outer join  
(SELECT
  promoCodeId AS promo_code1,
  LENGTH(lcPromoCodeId) AS promo_length,
  lcTitle AS promo_logic,
  promoCode AS promo_english,
  lcDescription AS promo_desc,
  promoCodeDiscount.promoCodeType AS promo_type,
  promoCodeDiscount.discountAmount AS flat_discount,
  promoCodeDiscount.discountPercent AS percent_discount,
  promoCodeDiscount.maxCap AS max_cap_on_promo,
  promoCodeDiscount.cashbackPercent AS CB_percent,
  promoCodeDiscount.cashbackAmount AS CB_amount,
  isCashback AS is_cb,
  promoCodeDiscount.offerPriceRange.FROM AS minimum_amount_se_valid,
  applicablePromoCodeDiscount.APP.promoCodeType as promo_type_App,
  applicablePromoCodeDiscount.APP.maxCap as max_cap_on_promo_App,
  applicablePromoCodeDiscount.APP.discountAmount as flat_discount_App,
  applicablePromoCodeDiscount.APP.discountPercent as percent_discount_App,
  applicablePromoCodeDiscount.APP.cashbackPercent as CB_percent_App,
  applicablePromoCodeDiscount.APP.cashbackAmount as CB_amount_App,
  applicablePromoCodeDiscount.WEB.promoCodeType as promo_type_Web, 
  applicablePromoCodeDiscount.WEB.discountAmount as flat_discount_Web, 
  applicablePromoCodeDiscount.WEB.discountPercent as percent_discount_Web, 
  applicablePromoCodeDiscount.WEB.maxCap as max_cap_on_promo_Web, 
  applicablePromoCodeDiscount.WEB.cashbackPercent as CB_percent_Web, 
  applicablePromoCodeDiscount.WEB.cashbackAmount as CB_amount_Web
FROM
  [big-query-1233:Atom.promocode] ) g 
on g.promo_code1 = a.Promo_code 
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

echo "promo table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com alka.gupta@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


