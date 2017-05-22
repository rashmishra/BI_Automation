#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH



v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive"
v_scripts_home="/home/ubuntu/BI_automation/deals_live"



while read line
do
cd ${v_scripts_home}/
./deals_live.sh $line 10
done < ${v_scripts_home}/deals.csv

cd ${v_grive_folder}
grive

echo "Guys,

TO check deals live today, 

Please follow the link: https://drive.google.com/drive/folders/0B59NPzbplo3LTVRNd0twQUJiV2s

Thanks in advance" | mutt -s "Updated deal data till $(date)" -c rashmi.mishra@nearbuy.com  alka.gupta@nearbuy.com anish.sinha@nearbuy.com  
-y

exit 0

