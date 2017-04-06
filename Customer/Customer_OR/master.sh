#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

while read line
do
cd /home/ubuntu/BI_automation/Customer/Customer_OR
./customer_or.sh $line 10
done < /home/ubuntu/BI_automation/Customer/Customer_OR/cust.csv
cd /home/ubuntu/BI/data/google_drive
grive

echo "Guys,

TO check Customer/Customer bought ORs only today, 

Please follow the link: https://drive.google.com/drive/folders/0B59NPzbplo3LY21zcjV0UHlmeW8

Thanks in advance" | mutt -s "Updated deal data till $(date)" -c rashmi.mishra@nearbuy.com  
-y

exit 0

