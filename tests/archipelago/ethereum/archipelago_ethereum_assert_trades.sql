-- Check if all archipelago trade events make it into the nft.trades
WITH raw_events AS (
    SELECT
        evt_block_time AS raw_block_time
        , evt_tx_hash AS raw_tx_hash
        , tradeId AS raw_unique_trade_id
    FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_Trade') }}
    WHERE evt_block_time >= '2022-6-20'
        AND evt_block_time < NOW() - INTERVAL '1 day' -- allow some head desync
)

, processed_events AS (
    SELECT
        block_time AS processed_block_time
        , tx_hash AS processed_tx_hash
        , unique_trade_id AS processed_trade_id
    FROM {{ ref('nft_trades') }}
    WHERE
        blockchain = 'ethereum'
        AND project = 'archipelago'
        AND version = 'v1'
        AND block_time >= '2022-6-20'
        AND block_time < NOW() - INTERVAL '1 day' -- allow some head desync
)

SELECT
    *
FROM raw_events
    OUTER JOIN processed_events AS n
        ON raw_block_time = processed_block_time AND raw_unique_trade_id = processed_trade_id
        WHERE NOT raw_unique_trade_id = processed_trade_id
