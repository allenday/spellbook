{{
    config(
        alias='tx_hash_labels_stable_to_stable_ethereum',
    )
}}

WITH
stable_to_stable_trades AS (
    SELECT DISTINCT tx_hash
    FROM {{ ref('cow_protocol_ethereum_trades') }}
    WHERE
        buy_token_address IN (
            SELECT contract_address
            FROM {{ ref('tokens_ethereum_erc20_stablecoins') }}
        )
        AND sell_token_address IN (
            SELECT contract_address
            FROM {{ ref('tokens_ethereum_erc20_stablecoins') }}
        )
)

SELECT
    tx_hash
    , "Stable to stable" AS name
    , "stable_to_stable" AS category
    , "gentrexha" AS contributor
    , "query" AS source
    , array("ethereum") AS blockchain
    , timestamp("2022-11-16") AS created_at
    , now() AS updated_at
FROM
    stable_to_stable_trades
