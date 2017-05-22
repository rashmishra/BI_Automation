#!/bin/bash

export TZ=Asia/Calcutta
#get the month name for the folder creation

MONTH_NAME=$(date +%b)
echo $MONTH_NAME

echo $1
echo $2


v_grive_folder="/home/ubuntu/BI_automation/BI/data/google_drive/Customer"

bq --format=csv query --n=10000000 "SELECT
  customerId,
  name,
  primaryEmailAddress,
  primaryPhoneNumber,
  isValidated,
  orderSummary.totalOrderAmount,
  orderSummary.totalOrderCount,
  createdAT
FROM (
  SELECT
    DATE(MSEC_TO_TIMESTAMP(createdAt)) AS createdAT,
    customerId,
    name,
    primaryEmailAddress,
    primaryPhoneNumber,
    sourceCreatedBy,
    orderSummary.totalOrderAmount,
    orderSummary.totalOrderCount,
    isValidated
  FROM (
    SELECT
      CASE
        WHEN LENGTH(STRING(createdat)) =10 THEN createdAt*1000+19800000
        ELSE createdAT+19800000
      END AS createdAt,
      sourceCreatedBy,
      customerId,
      orderSummary.totalOrderAmount,
      orderSummary.totalOrderCount,
      isValidated,
      name,
      primaryEmailAddress,
      primaryPhoneNumber
    FROM
      Atom.customer
    WHERE
      isValidated=TRUE
      AND orderSummary.totalOrderCount IS NULL ) )
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
ORDER BY
  1 DESC
" > ${v_grive_folder}/$1.csv
 # /home/ubuntu/BI/data/google_drive/Customer/$1.csv




exit 0

