{{ config(
    alias = 'events_close_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'price', 'payout', 'perc_closed']
    )
}}

WITH

close_position_v2 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
        , _closePrice / 1e18 AS price
        , _payout / 1e18 AS payout
        , _percent / 1e8 AS perc_closed
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v3 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
        , _closePrice / 1e18 AS price
        , _payout / 1e18 AS payout
        , _percent / 1e8 AS perc_closed
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v4 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
        , _closePrice / 1e18 AS price
        , _payout / 1e18 AS payout
        , _percent / 1e8 AS perc_closed
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, close_position_v5 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
        , _closePrice / 1e18 AS price
        , _payout / 1e18 AS payout
        , _percent / 1e8 AS perc_closed
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV5_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)


SELECT
    *
    , 'v2' AS version
FROM close_position_v2

UNION ALL

SELECT
    *
    , 'v3' AS version
FROM close_position_v3

UNION ALL

SELECT
    *
    , 'v4' AS version
FROM close_position_v4

UNION ALL

SELECT
    *
    , 'v5' AS version
FROM close_position_v5
