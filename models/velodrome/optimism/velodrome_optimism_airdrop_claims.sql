{{
    config(
        alias='airdrop_claims',
        materialized = 'view',
                        unique_key = ['recipient', 'tx_hash', 'evt_index']
    )
}}

{% set velo_token_address = '0x3c8b650257cfb5f272f799f5e2b4e65093a11a05' %}

WITH price_bounds AS (
    SELECT MIN(hour) AS min_hour
    , MAX(hour) AS max_hour
    , ARRAY_AGG(median_price ORDER BY hour ASC LIMIT 1)[OFFSET(0)] AS min_price
    , ARRAY_AGG(median_price ORDER BY hour DESC LIMIT 1)[OFFSET(0)] AS max_price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'optimism'
    AND contract_address='{{velo_token_address}}'
    )

SELECT 'optimism' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Velodrome' AS project
, 'Velodrome Airdrop' AS airdrop_identifier
, t.to AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS BIGNUMERIC) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS FLOAT64) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT min_hour FROM price_bounds) AND t.evt_block_time <= (SELECT max_hour FROM price_bounds) THEN CAST(pu.median_price*t.amount/POWER(10, 18) AS FLOAT64)
    WHEN t.evt_block_time < (SELECT min_hour FROM price_bounds) THEN CAST((SELECT min_price FROM price_bounds)*t.amount/POWER(10, 18) AS FLOAT64)
    WHEN t.evt_block_time > (SELECT max_hour FROM price_bounds) THEN CAST((SELECT max_price FROM price_bounds)*t.amount/POWER(10, 18) AS FLOAT64)
    END AS amount_usd
, '{{velo_token_address}}' AS token_address
, 'VELO' AS token_symbol
, t.evt_index
FROM {{ source('velodrome_optimism', 'MerkleClaim_evt_Claim') }} t
LEFT JOIN {{ ref('dex_prices') }} pu ON pu.blockchain = 'optimism'
    AND pu.contract_address='{{velo_token_address}}'
    AND pu.hour = TIMESTAMP_TRUNC(t.evt_block_time, hour)
    {% if is_incremental() %}
    AND pu.hour >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}