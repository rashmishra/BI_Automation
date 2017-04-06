#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

while read line
do
cd /home/ubuntu/BI_automation/cancellations
./cancellations_new.sh $line 10
done < /home/ubuntu/BI_automation/cancellations/CMList.csv
cd /home/ubuntu/BI/data/google_drive
grive

echo "Guys,


Please follow the link for cancellations of this month: https://drive.google.com/drive/folders/0B59NPzbplo3LUUF3dVRpbFFQc00


Thanks in advance" | mutt -s "Updated sales data till $(date)" -c rashmi.mishra@nearbuy.com  alka.gupta@nearbuy.com anish.sinha@nearbuy.com
-y

exit 0

