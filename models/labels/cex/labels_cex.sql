{{config(alias='cex')}}

SELECT * FROM {{ ref('labels_cex_ethereum') }}

UNION All

-- add address list from CEXs
SELECT 
array("optimism"), address, distinct_name, 'cex', 'msilb7','static','2022-10-10'::timestamp,CURRENT_TIMESTAMP
FROM {{ ref('addresses_optimism_cex') }}