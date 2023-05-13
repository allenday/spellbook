{{ config(
    schema = 'tigris_v2_arbitrum',
    alias = 'events_add_margin',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin']
    )
}}

WITH

add_margin_v1 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        id AS position_id,
        addmargin / 1e18 AS margin_change,
        newmargin / 1e18 AS margin,
        newprice / 1e18 AS price,
        trader
    FROM
        {{ source('tigristrade_v2_arbitrum', 'Trading_evt_AddToPosition') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

add_margin_v2 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        id AS position_id,
        addmargin / 1e18 AS margin_change,
        newmargin / 1e18 AS margin,
        newprice / 1e18 AS price,
        trader
    FROM
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_evt_AddToPosition') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)


SELECT
    *,
    'v2.1' AS version
FROM add_margin_v1

UNION ALL

SELECT
    *,
    'v2.2' AS version
FROM add_margin_v2
