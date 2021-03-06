#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive";
v_scripts_home="/home/ubuntu/BI_automation/BI/scripts";


while read line
do
cd ${v_scripts_home}/CM_Reports
./dailyreports.sh $line 10
done < ${v_scripts_home}/CM_Reports/CMList.csv
cd "${v_grive_folder}/"
grive

echo "Guys,

This has yesterday's performance. Please do the following today

Please follow the link: https://drive.google.com/open?id=0B59NPzbplo3LS2hlQjVBem9qVzQ

1. Download the workbook on your desktop
2. Make Basic pivot on Sales rep, merchants, and offers
3. Share that as daily sales report with your teams

You can cut this data into a lot of other ways
Thanks in advance" | mutt -s "Updated sales data till $(date)" -c rashmi.mishra@nearbuy.com rahul.sachan@nearbuy.com  sairanganath.v@nearbuy.com    ashish.singh@nearbuy.com  -y

exit 0
