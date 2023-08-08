{{config(alias='aggregators', materialized = 'view', file_format = 'delta')}}

SELECT contract_address, name
FROM {{ ref('nft_ethereum_aggregators_manual')}}
UNION ALL -- no union all to resolve any duplicates
SELECT contract_address, name
FROM {{ ref('nft_ethereum_aggregators_gem')}}