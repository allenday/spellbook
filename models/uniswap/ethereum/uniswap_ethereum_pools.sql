{{ config(
    schema = 'uniswap_ethereum',
    alias = 'pools',
    materialized = 'view',
            unique_key = ['pool']
    )
}}

SELECT 'ethereum' AS blockchain
, 'uniswap' AS project
, 'v2' AS version
, pair AS pool
, 0.3 AS fee
, token0
, token1
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('uniswap_v2_ethereum', 'Factory_evt_PairCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'ethereum' AS blockchain
, 'uniswap' AS project
, 'v3' AS version
, pool
, CAST(fee AS numeric)
, token0
, token1
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}