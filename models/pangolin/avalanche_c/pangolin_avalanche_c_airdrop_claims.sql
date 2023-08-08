{{
    config(
        alias='airdrop_claims',
        materialized = 'view',
                tags=['static'],
        unique_key = ['recipient', 'tx_hash', 'evt_index']
    )
}}

{% set png_token_address = '0x60781c2586d68229fde47564546784ab3faca982' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , ARRAY_AGG(price ORDER BY minute ASC LIMIT 1)[OFFSET(0)] AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'avalanche_c'
    AND contract_address='{{png_token_address}}'
    )

SELECT 'avalanche_c' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Pangolin' AS project
, 'Pangolin Airdrop' AS airdrop_identifier
, t.claimer AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS BIGNUMERIC) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS FLOAT64) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS FLOAT64)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS FLOAT64)
    END AS amount_usd
, '{{png_token_address}}' AS token_address
, 'PNG' AS token_symbol
, t.evt_index
FROM {{ source('pangolin_exchange_avalanche_c', 'Airdrop_evt_PngClaimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'avalanche_c'
    AND pu.contract_address='{{png_token_address}}'
    AND pu.minute=TIMESTAMP_TRUNC(t.evt_block_time, minute)
WHERE t.evt_block_time BETWEEN '2021-02-09' AND '2021-03-10'