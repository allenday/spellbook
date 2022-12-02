{{config(alias='tornado_cash',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "bnb", "avalanche_c", "optimism", "gnosis"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}'
)}}

WITH tornado_addresses AS (
    SELECT
        lower(blockchain) AS blockchain
        , tx_hash
        , depositor AS address
        , 'Depositor' AS name
    FROM {{ ref('tornado_cash_deposits') }}
    UNION
    SELECT
        lower(blockchain) AS blockchain
        , tx_hash
        , recipient AS address
        , 'Recipient' AS name
    FROM {{ ref('tornado_cash_withdrawals') }}
)

SELECT
    address
    , 'tornado_cash' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , collect_set(blockchain) AS blockchain
    , 'Tornado Cash ' || array_join(collect_set(name), ' AND ') AS name
    , timestamp('2022-10-01') AS created_at
    , now() AS updated_at
FROM tornado_addresses
GROUP BY address
