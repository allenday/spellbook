{{config(alias='cex')}}

SELECT * FROM {{ ref('labels_cex_ethereum') }}

UNION All

-- add address list from CEXs
SELECT 
ARRAY("optimism"), address, distinct_name, 'cex', 'msilb7','static',CAST('2022-10-10' AS TIMESTAMP),CURRENT_TIMESTAMP
FROM {{ ref('addresses_optimism_cex') }}
