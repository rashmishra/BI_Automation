#!/bin/bash

export TZ=Asia/Calcutta
TASK_START_TIME=`date`
echo $TASK_START_TIME

bq query --append=1 --flatten_results=1 --allow_large_results=1 --destination_table=BI_Automation.mapping_history "select CURRENT_DATE() as date,id, type, mappings.chain.id, mappings.businessAccount.id, mappings.merchant.id
from flatten(flatten(Atom.mapping,mappings.businessAccount),mappings.chain)"

TASK_END_TIME=`date`
echo $TASK_END_TIME

mutt -s "Mapping history table updated $TASK_END_TIME" rahul.sachan@nearbuy.com < /dev/null
