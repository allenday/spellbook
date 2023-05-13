{{ config(
    schema = 'tigris_v2_arbitrum',
    alias = 'events_close_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'price', 'payout', 'perc_closed']
    )
}}

WITH

close_position_v1 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        id AS position_id,
        closeprice / 1e18 AS price,
        payout / 1e18 AS payout,
        percent / 1e8 AS perc_closed,
        trader
    FROM
        {{ source('tigristrade_v2_arbitrum', 'Trading_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

close_position_v2 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        id AS position_id,
        closeprice / 1e18 AS price,
        payout / 1e18 AS payout,
        percent / 1e8 AS perc_closed,
        trader
    FROM
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_evt_PositionClosed') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    *,
    '2.1' AS version
FROM close_position_v1

UNION ALL

SELECT
    *,
    '2.2' AS version
FROM close_position_v2
