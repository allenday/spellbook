{{config(alias='cex')}}

SELECT * FROM {{ ref('labels_cex_ethereum') }}

UNION ALL

SELECT * FROM {{ ref('labels_cex_bnb') }}

UNION ALL

-- add address list FROM CEXs
SELECT
    array("optimism")
    , address
    , distinct_name
    , "cex"
    , "msilb7"
    , "static"
    , "2022-10-10"::timestamp
    , now()
FROM {{ ref('addresses_optimism_cex') }}
