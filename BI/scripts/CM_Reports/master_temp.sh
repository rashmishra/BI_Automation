#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
while read line
do
cd /home/ubuntu/BI/scripts/CM_Reports
./dailyreports_temp.sh $line 
done < /home/ubuntu/BI/scripts/CM_Reports/CMList.csv
cd /home/ubuntu/BI/data/google_drive
grive

exit 0
