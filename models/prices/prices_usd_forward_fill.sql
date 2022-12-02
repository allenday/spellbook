{{ config(
        schema='prices',
        alias ='usd_forward_fill',
        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "polygon"]\',
                                    "sector",
                                    "prices",
                                    \'["0xRob"]\') }}'
        )
}}

-- how much time we look back, anything before is considered finalized, anything after is forward filled.
-- we could decrease this to optimize query performance but it's a tradeoff with resiliency to lateness.
{%- set lookback_interval = '2 day' %}


WITH
finalized AS (
    SELECT *
    FROM {{ source('prices', 'usd') }}
    WHERE minute <= now() - INTERVAL {{ lookback_interval }}
)

, unfinalized AS (
    SELECT
        *
        , lead(
            minute
        ) OVER (
            PARTITION BY
                blockchain, contract_address, decimals, symbol
            ORDER BY minute ASC
        ) AS next_update_minute
    FROM {{ source('prices', 'usd') }}
    WHERE minute > now() - INTERVAL {{ lookback_interval }}
)

, timeseries AS (
    SELECT explode(sequence(
        date_trunc('minute', now() - INTERVAL {{ lookback_interval }})
        , date_trunc('minute', now())
        , INTERVAL 1 MINUTE)) AS minute
)

, forward_fill AS (
    SELECT
        timeseries.minute
        , blockchain
        , contract_address
        , decimals
        , symbol
        , price
    FROM timeseries
    LEFT JOIN unfinalized
        -- perform forward fill
        ON
            timeseries.minute >= unfinalized.minute AND (
                unfinalized.next_update_minute IS NULL OR timeseries.minute < unfinalized.next_update_minute
            )
)

SELECT
    minute
    , blockchain
    , contract_address
    , decimals
    , symbol
    , price
FROM finalized
UNION ALL
SELECT
    minute
    , blockchain
    , contract_address
    , decimals
    , symbol
    , price
FROM forward_fill
