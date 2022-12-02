{{
    config(
        alias='tx_hash_labels_stable_to_stable_ethereum',
    )
}}

with
stable_to_stable_trades AS (
    SELECT
    distinct tx_hash
    FROM {{ ref('cow_protocol_ethereum_trades') }}
    where buy_token_address in (SELECT contract_address FROM {{ ref('tokens_ethereum_erc20_stablecoins') }})
        AND sell_token_address in (SELECT contract_address FROM {{ ref('tokens_ethereum_erc20_stablecoins') }})
)
SELECT
    array("ethereum") AS blockchain
    , tx_hash
    , "Stable to stable" AS name
    , "stable_to_stable" AS category
    , "gentrexha" AS contributor
    , "query" AS source
    , timestamp('2022-11-16') AS created_at
    , now() AS updated_at
FROM
    stable_to_stable_trades