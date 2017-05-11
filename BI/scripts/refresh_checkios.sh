bq query --maximum_billing_tier 2 --append_table --destination_table=bitest.checkios "SELECT region, city, transactionid, transactions FROM bi.ios_txns_in_GA_yesterday group by 1,2,3,4" 
