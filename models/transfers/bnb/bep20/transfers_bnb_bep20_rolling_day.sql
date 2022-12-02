{{ config(
        alias ='bep20_rolling_day'
        )
}}

SELECT
    'bnb' AS blockchain
    , day
    , wallet_address
    , token_address
    , symbol
    , current_timestamp() AS last_updated
    , ROW_NUMBER() OVER (PARTITION BY token_address, wallet_address ORDER BY day DESC) AS recency_index
    , sum(amount_raw) OVER (
        PARTITION BY token_address, wallet_address ORDER BY day
    ) AS amount_raw
    , sum(amount) OVER (
        PARTITION BY token_address, wallet_address ORDER BY day
    ) AS amount
FROM {{ ref('transfers_bnb_bep20_agg_day') }}
