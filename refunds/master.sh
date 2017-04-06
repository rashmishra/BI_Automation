#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

while read line
do
cd /home/ubuntu/BI_automation/refunds
./refunds.sh $line 10
done < /home/ubuntu/BI_automation/refunds/CMList.csv
cd /home/ubuntu/BI/data/google_drive
grive

echo "Guys,


Please follow the link for refunds of this month: https://drive.google.com/drive/folders/0B59NPzbplo3LX2o3SEhOUllCZ1E

You can cut this data into a lot of other ways
Thanks in advance" | mutt -s "Updated sales data till $(date)" -c rashmi.mishra@nearbuy.com ##akhil.dhingra@nearbuy.com ankur.sarawagi@nearbuy.com  
alka.gupta@nearbuy.com anish.sinha@nearbuy.com -y

exit 0

