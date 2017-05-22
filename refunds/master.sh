#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive"
v_scripts_home="/home/ubuntu/BI_automation/refunds"


while read line
do
cd ${v_scripts_home}
./refunds.sh $line 10
done < ${v_scripts_home}/CMList.csv
cd ${v_grive_folder}
grive

echo "Guys,


Please follow the link for refunds of this month: https://drive.google.com/drive/folders/0B59NPzbplo3LX2o3SEhOUllCZ1E

You can cut this data into a lot of other ways
Thanks in advance" | mutt -s "Updated sales data till $(date)" -c rashmi.mishra@nearbuy.com alka.gupta@nearbuy.com anish.sinha@nearbuy.com -y

exit 0

