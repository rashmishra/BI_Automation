v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=hadoop_data_pipeline;
date


# redemption table loading. Replace existing
v_query_order="SELECT oh.orderid AS orderid, 

       oh.customerid AS customerid, 

       oh.createdat AS createdat, 

       oh.promocode AS promocode, 

       oh.source AS source, 

       oh.referralprogramid AS referralprogramid, 

       oh.deviceid AS deviceid, 

       oh.appversion AS appversion, 

       oh.totalprice AS totalprice, 

       oh.buyinglat AS buyinglat, 

       oh.buyinglong AS buyinglong, 

       oh.totalcashbackamount AS totalcashbackamount, 

       oh.creditsrequested AS creditsrequested, 

       oh.payable AS payable,  

       categoryid, 

       dealid, 

       offerid, 

       offertitle, 

       merchantid,
	
       b.status as status,

       EXACT_COUNT_DISTINCT(b.orderlineid) AS no_of_vouchers

FROM   Atom.order_header oh 

       LEFT JOIN Atom.order_line b 

              ON oh.orderid = b.orderid 

WHERE  oh.ispaid = 't' 

       AND b.unitprice > 0 

       AND b.status IN( 14,  15 ) 

       AND DATE(MSEC_TO_TIMESTAMP(oh.createdat + 19800000)) >= DATE('2016-03-01')

GROUP BY orderid, 

        customerid, 

       createdat, 

       promocode, 

       source, 

       referralprogramid, 

       deviceid, 

       appversion, 

       totalprice, 

       buyinglat, 

       buyinglong, 

       totalcashbackamount, 

       creditsrequested, 

       payable,  

       categoryid, 

       dealid, 

       offerid, 

       offertitle, 

       merchantid,

       status
          
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=order
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_order\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_order" &
v_first_pid=$!
v_order_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_order_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_order_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Order Table status in Hadoop dataset:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ankit.kohli@nearbuy.com  ##sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


