{{ config(
        schema='prices_optimism',
        alias ='tokens_bridged',
        materialized='table',
        file_format = 'delta',
        tags=['static']
        )
}}
SELECT
    e.token_id,
    'optimism' AS blockchain,
    e.symbol AS symbol,
    o.l2_token AS contract_address,
    e.decimals
FROM {{ ref('tokens_optimism_erc20_bridged_mapping') }} AS o
INNER JOIN {{ ref('prices_ethereum_tokens') }} AS e
    ON e.contract_address = o.l1_token
