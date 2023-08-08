{{
    config(
        alias='airdrop_claims',
        materialized = 'view',
                tags=['static'],
        unique_key = ['recipient', 'tx_hash', 'evt_index']
    )
}}

{% set giv_token_address = '0x4f4f9b8d5b4d0dc10506e5551b0513b61fd59e75' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , ARRAY_AGG(price ORDER BY minute ASC LIMIT 1)[OFFSET(0)] AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'gnosis'
    AND contract_address='{{giv_token_address}}'
    )

SELECT 'gnosis' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Giveth' AS project
, 'Giveth Airdrop' AS airdrop_identifier
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS BIGNUMERIC) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS FLOAT64) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS FLOAT64)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS FLOAT64)
    END AS amount_usd
, '{{giv_token_address}}' AS token_address
, 'GIV' AS token_symbol
, t.evt_index
FROM {{ source('giveth_gnosis', 'MerkleDistro_evt_Claimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'gnosis'
    AND pu.contract_address='{{giv_token_address}}'
    AND pu.minute=TIMESTAMP_TRUNC(t.evt_block_time, minute)
WHERE t.evt_block_time BETWEEN '2021-12-24' AND '2022-12-31'