#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

while read line
do
cd /home/ubuntu/BI_automation/deals_live
./deals_live.sh $line 10
done < /home/ubuntu/BI_automation/deals_live/deals.csv
cd /home/ubuntu/BI/data/google_drive
grive

echo "Guys,

TO check deals live today, 

Please follow the link: https://drive.google.com/drive/folders/0B59NPzbplo3LTVRNd0twQUJiV2s

Thanks in advance" | mutt -s "Updated deal data till $(date)" -c rashmi.mishra@nearbuy.com  alka.gupta@nearbuy.com anish.sinha@nearbuy.com  
-y

exit 0

