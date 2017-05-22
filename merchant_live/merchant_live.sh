#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

MONTH_NAME=$(date +%b)
echo $MONTH_NAME

echo $1
echo $2

v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive/merchant_live"

bq --format=csv query --n=10000000 "select 
a.merchantid,
a.name,
AddLine1,
Locality,
City,
State,
Lat,
Long,
Mall,
Market,
Street,
a.Category,
case when b.merchantid is not null then 'Yes' else 'No' end isDealCurrentlyLive from 
(

SELECT
  merchantId,
  name,
  redemptionAddress.addressLine1 AS AddLine1,
  redemptionAddress.attribute_2 AS Locality,
  redemptionAddress.cityTown AS City,
  redemptionAddress.state AS State,
  redemptionAddress.latitude AS Lat,
  redemptionAddress.attribute_2 AS Long,
  redemptionAddress.attribute_7 AS Mall,
  redemptionAddress.attribute_8 AS Market,
  redemptionAddress.street AS Street,
  catInfo.key AS Category,
FROM
  [Atom.merchant]
WHERE
  isChain = FALSE AND isPublished = TRUE and isActive = TRUE
  ) a
left join 
(select _id as dealId, msec_to_timestamp(units.dval.dFrmDt+19800000) as dealStartDate, msec_to_timestamp(units.dval.dToDt+19800000) as dealEndDate, 
mapd_merchant_id as merchantId from Atom.deal
left join Atom_simplified.mapping_deal mpd on mpd.mapd_deal_id =_id
where units.dval.dFrmDt  is not null and units.dval.dToDt is not null
and INTEGER(left(STRING(now()),13)) between units.dval.dFrmDt  and units.dval.dToDt) b on b.merchantid = a.merchantid
  
  GROUP BY 
  a.merchantid,
a.name,
AddLine1,
Locality,
City,
State,
Lat,
Long,
Mall,
Market,
Street,
a.Category,
isDealCurrentlyLive" > ${v_grive_folder}/merchant_live.csv

exit 0

