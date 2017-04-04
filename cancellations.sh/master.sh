#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

while read line
do
cd /home/ubuntu/BI_automation/cancellations
./cancellations.sh $line 10
done < /home/ubuntu/BI_automation/cancellations/CMList.csv
cd /home/ubuntu/BI/data/google_drive
grive

echo "Guys,

This has yesterday's performance. Please do the following today

Please follow the link: https://drive.google.com/drive/folders/0Bw76R706sm29QWNWLTI5b3Nqc28

1. Download the workbook on your desktop
2. Make Basic pivot on Sales rep, merchants, and offers
3. Share that as daily sales report with your teams

You can cut this data into a lot of other ways
Thanks in advance" | mutt -s "Updated sales data till $(date)" -c rashmi.mishra@nearbuy.com akhil.dhingra@nearbuy.com ankur.sarawagi@nearbuy.com  alka.gupta@nearbuy.com anish.sinha@nearbuy.com
-y

exit 0

