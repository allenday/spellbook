{{ config(
        schema='prices',
        alias ='usd_latest'
        )
}}

SELECT
  pu.blockchain
, pu.contract_address
, pu.decimals
, pu.symbol
, max(pu.minute) as minute
, ARRAY_AGG(pu.price ORDER BY pu.minute DESC LIMIT 1)[OFFSET(0)] as price
FROM {{ source('prices', 'usd') }} pu
GROUP BY 1,2,3,4