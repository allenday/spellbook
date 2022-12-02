{{ config(
        alias ='bep20_rolling_hour'
        )
}}

SELECT
    'bnb' AS blockchain
    , hour
    , wallet_address
    , token_address
    , symbol
    , current_timestamp() AS last_updated
    , ROW_NUMBER() OVER (PARTITION BY token_address, wallet_address ORDER BY hour DESC) AS recency_index
    , sum(amount_raw) OVER (
        PARTITION BY token_address, wallet_address ORDER BY hour
    ) AS amount_raw
    , sum(amount) OVER (
        PARTITION BY token_address, wallet_address ORDER BY hour
    ) AS amount
FROM {{ ref('transfers_bnb_bep20_agg_hour') }}
