#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH


v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive"
v_scripts_home="/home/ubuntu/BI_automation/Customer/Customer_OR"

while read line
do
cd ${v_scripts_home}/
./customer_or.sh $line 10
done < ${v_scripts_home}/cust.csv

cd ${v_grive_folder}
grive

echo "Guys,

TO check Customer/Customer bought ORs only today, 

Please follow the link: https://drive.google.com/drive/folders/0B59NPzbplo3LY21zcjV0UHlmeW8

Thanks in advance" | mutt -s "Updated deal data till $(date)" -c rashmi.mishra@nearbuy.com  
-y

exit 0

