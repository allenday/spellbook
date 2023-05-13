{{ config(
        schema='prices_optimism',
        alias ='tokens',
        materialized='table',
        file_format = 'delta',
        tags=['static']
        )
}}
SELECT
    token_id,
    blockchain,
    symbol,
    contract_address,
    decimals
FROM {{ ref('prices_optimism_tokens_curated') }}
UNION ALL
SELECT
    token_id,
    blockchain,
    symbol,
    contract_address,
    decimals
FROM {{ ref('prices_optimism_tokens_bridged') }}
WHERE contract_address NOT IN (SELECT contract_address FROM {{ ref('prices_optimism_tokens_curated') }})
