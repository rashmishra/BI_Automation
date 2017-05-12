#!/bin/bash

bq query --replace=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.dealsCategorization "SELECT  
  supersuperset.dealId as dealId,
  supersuperset.dealCat as dealCat,
  CASE WHEN (dov.DealID IS NULL
    OR (dov.DealID IS NOT NULL
      AND dov.FinalCategoryName IS NULL)) THEN pCatName ELSE dov.FinalCategoryName END AS finalCategoryName,
  supersuperset.merchantId as merchantId,
  supersuperset.merchantName as merchantName,
  minOfferPrice/100 AS minOfferPriceInRs,
  CASE WHEN (dov.DealID IS NULL
    OR (dov.DealID IS NOT NULL
      AND dov.offerType IS NULL)) THEN CASE WHEN minOfferPrice/100<2 THEN \"Raffle\" WHEN minOfferPrice/100<100 THEN \"Option\" ELSE \"Full\" END ELSE dov.offerType END AS offerType
FROM (
  SELECT
    STRING(dealId) AS dealId,
    dealCat,
    pCatName,
    merchantId,
    merchantName,
    catH.priority_order AS sortOrder,
    MIN(catH.priority_order) OVER (PARTITION BY dealId) AS minSortOrder,
    catCount,
    minOfferPrice
  FROM (SELECT
      subset.dealId AS dealId,
      subset.dealCat AS dealCat,
      merchantId AS merchantId,
      merchantName AS merchantName,
      nileDeals.mappedCategories.name AS pCatName,
      catCount,
      minOfferPrice
    FROM
      FLATTEN([Atom.nile], mappedCategories) nileDeals
    LEFT JOIN (
      SELECT
        nd._id AS dealId,
        category._id AS dealCat,
        nd.merchant._id as merchantId,
        nd.merchant.name as merchantName,
        MIN(ao.offers.units.ps.msp) OVER (PARTITION BY ao._id) AS minOfferPrice,
        ao.offers.units.ps.msp AS offerPrice,
        CASE WHEN mappedCategories.path IS NULL THEN COUNT(mappedCategories.displayName) ELSE NULL END AS catCount
      FROM
        FLATTEN([Atom.nile],merchant) nd
      INNER JOIN
        FLATTEN([Atom.offer],offers) ao
      ON
        nd._id = ao._id WHERE
        mappedCategories.path IS NULL
        AND category._id NOT IN (\"GTW\")
      GROUP BY
        dealId,
        dealCat,
        mappedCategories.path,
        offerPrice,
        ao._id,
        ao.offers.units.ps.msp,
        merchantId,
        merchantName
      ORDER BY
        1 ASC,
        4 DESC) subset
    ON
      nileDeals._id = subset.dealId
    WHERE
      minOfferPrice = offerPrice) superset
  LEFT JOIN
    dbdev.dealcategory catH
  ON
    catH.Category_Name = superset.pCatName) supersuperset
LEFT JOIN
  [dbdev.dealsOverride] dov
ON
  dov.DealID = supersuperset.dealId
WHERE
  NVL(sortOrder,0) = NVL(minSortOrder,0)
ORDER BY
  supersuperset.dealId" > /dev/null


bq query --replace=1 --flatten_results=1 --allow_large_results=1 --destination_table=bi.allDealsCategory "SELECT dealId,
dealCat,
finalCategoryName,
merchantName,
minOfferPriceInRs,
offerType
FROM (
select nullCat.dealId as dealId,
nullCat.dealCat as dealCat,
notNullCat.finalCategoryName as finalCategoryName,
nullCat.merchantName as merchantName,
nullCat.minOfferPriceInRs as minOfferPriceInRs,
nullCat.offerType as offerType
from 
(select * 
from bi.dealsCategorization
where finalCategoryName is null
) nullCat
LEFT JOIN 
(select * 
from bi.dealsCategorization
where finalCategoryName is not null
) notNullCat
ON nullCat.merchantId = notNullCat.merchantId
GROUP BY
dealId,
dealCat,
finalCategoryName,
merchantName,
minOfferPriceInRs,
offerType
),
(
SELECT dealId,
dealCat,
finalCategoryName,
merchantName,
minOfferPriceInRs,
offerType
from bi.dealsCategorization
where finalCategoryName is not null
GROUP BY 
dealId,
dealCat,
finalCategoryName,
merchantName,
minOfferPriceInRs,
offerType),
(
SELECT
      STRING(subset.dealId) AS dealId,
      subset.dealCat AS dealCat,
      merchantName,
      \"Travel\" AS finalCategoryName,
      minOfferPrice/100 as minOfferPriceInRs,
      \"Full\" as offerType
    FROM
      [Atom.nile] nileDeals
    LEFT JOIN (
      SELECT
        nd._id AS dealId,
        category._id AS dealCat,
        nd.merchant._id as merchantId,
        nd.merchant.name as merchantName,
        MIN(ao.offers.units.ps.msp) OVER (PARTITION BY ao._id) AS minOfferPrice,
        ao.offers.units.ps.msp AS offerPrice
      FROM
        FLATTEN([Atom.nile],merchant) nd
      INNER JOIN
        FLATTEN([Atom.offer],offers) ao
      ON
        nd._id = ao._id WHERE
        category._id =\"GTW\"
      GROUP BY
        dealId,
        dealCat,
        merchantId,
        offerPrice,
        ao._id,
        ao.offers.units.ps.msp,
        merchantId,
        merchantName
      ORDER BY
        1 ASC,
        4 DESC) subset
    ON
      nileDeals._id = subset.dealId
    WHERE
      minOfferPrice = offerPrice
      GROUP BY 
      dealId,
      dealCat,
      merchantName,
      finalCategoryName,
      minOfferPriceInRs)" > /dev/null

