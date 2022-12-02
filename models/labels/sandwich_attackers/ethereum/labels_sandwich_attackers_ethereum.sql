{{config(alias='sandwich_attackers_ethereum')}}

WITH
eth_sandwich_attackers AS (
    SELECT DISTINCT buy.tx_to AS address
    FROM {{ ref('dex_trades') }} AS buy
    INNER JOIN {{ ref('dex_trades') }} AS sell
        ON sell.block_time = buy.block_time
            AND sell.tx_hash != buy.tx_hash
            AND buy.tx_from = sell.tx_from
            AND buy.tx_to = sell.tx_to
            AND buy.project_contract_address = sell.project_contract_address
            AND buy.token_bought_address = sell.token_sold_address
            AND buy.token_sold_address = sell.token_bought_address
            AND buy.token_bought_amount_raw = sell.token_sold_amount_raw
    INNER JOIN {{ source('ethereum', 'transactions') }} AS et_buy
        ON et_buy.hash = buy.tx_hash
    INNER JOIN {{ source('ethereum', 'transactions') }} AS et_sell
        ON et_sell.hash = sell.tx_hash
    WHERE
        buy.blockchain = 'ethereum'
        AND sell.blockchain = 'ethereum'
        AND (et_sell.index >= et_buy.index + 2 -- buy first
            OR et_buy.index >= et_sell.index + 2) -- sell first
        -- uniswap v2 router
        AND buy.tx_to != '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
        -- uniswap v3 router
        AND buy.tx_to != '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45'
)

SELECT
    address
    , 'Sandwich Attacker' AS name
    , 'sandwich_attackers' AS category
    , 'alexth' AS contributor
    , 'query' AS source
    , array('ethereum') AS blockchain
    , timestamp('2022-10-14') AS created_at
    , now() AS updated_at
FROM
    eth_sandwich_attackers
