
#!/bin/bash
#Get the month number to be passed in the query

export TZ=Asia/Calcutta
MNTH=$(date +%m)
echo $MNTH


v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive"
v_scripts_home="/home/ubuntu/BI_automation/web_txn"


while read line
do
cd "${v_scripts_home}/"
./Daily_Web_Transactions.sh $line 10
done < ${v_scripts_home}/data.csv
cd "${v_grive_folder}/"
grive

echo "Guys,


Please follow the link for Daily_Web_Transactions of this month: https://drive.google.com/drive/folders/0B59NPzbplo3LNWpPT2cyek5palU


Thanks in advance" | mutt -s "Updated sales data till $(date)" -c rashmi.mishra@nearbuy.com   -y

exit 0
