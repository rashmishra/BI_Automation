
#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH

while read line
do
cd /home/ubuntu/BI_automation/web_txn
./Daily_Web_Transactions.sh $line 10
done < /home/ubuntu/BI_automation/web_txn/data.csv
cd /home/ubuntu/BI/data/google_drive
grive

echo "Guys,


Please follow the link for Daily_Web_Transactions of this month: https://drive.google.com/drive/folders/0B59NPzbplo3LNWpPT2cyek5palU


Thanks in advance" | mutt -s "Updated sales data till $(date)" -c rashmi.mishra@nearbuy.com  
-y

exit 0
