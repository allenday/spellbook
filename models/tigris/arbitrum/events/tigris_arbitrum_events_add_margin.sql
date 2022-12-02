{{ config(
    alias = 'events_add_margin',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin']
    )
}}

WITH

add_margin_v2 AS (
    SELECT
        ap.evt_tx_hash
        , ap.evt_index
        , ap.evt_block_time
        , ap._id AS position_id
        , ap._trader AS trader
        , date_trunc('day', ap.evt_block_time) AS day
        , af._addMargin / 1e18 AS margin_change
        , ap._newMargin / 1e18 AS margin
        , ap._newPrice / 1e18 AS price
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_AddToPosition') }} AS ap
    INNER JOIN
        {{ source('tigristrade_arbitrum', 'TradingV2_call_addToPosition') }} AS af
        ON ap._id = af._id
            AND ap.evt_tx_hash = af.call_tx_hash
            AND af.call_success = true
            {% if is_incremental() %}
                AND af.call_block_time >= date_trunc(
                    'day', now() - INTERVAL '1 week'
                )
            {% endif %}
            {% if is_incremental() %}
                WHERE
                    ap.evt_block_time >= date_trunc(
                        'day', now() - INTERVAL '1 week'
                    )
            {% endif %}
)

, add_margin_v3 AS (
    SELECT
        ap.evt_tx_hash
        , ap.evt_index
        , ap.evt_block_time
        , ap._id AS position_id
        , ap._trader AS trader
        , date_trunc('day', ap.evt_block_time) AS day
        , af._addMargin / 1e18 AS margin_change
        , ap._newMargin / 1e18 AS margin
        , ap._newPrice / 1e18 AS price
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_AddToPosition') }} AS ap
    INNER JOIN
        {{ source('tigristrade_arbitrum', 'TradingV3_call_addToPosition') }} AS af
        ON ap._id = af._id
            AND ap.evt_tx_hash = af.call_tx_hash
            AND af.call_success = true
            {% if is_incremental() %}
                AND af.call_block_time >= date_trunc(
                    'day', now() - INTERVAL '1 week'
                )
            {% endif %}
            {% if is_incremental() %}
                WHERE
                    ap.evt_block_time >= date_trunc(
                        'day', now() - INTERVAL '1 week'
                    )
            {% endif %}
)

, add_margin_v4 AS (
    SELECT
        ap.evt_tx_hash
        , ap.evt_index
        , ap.evt_block_time
        , ap._id AS position_id
        , ap._trader AS trader
        , date_trunc('day', ap.evt_block_time) AS day
        , af._addMargin / 1e18 AS margin_change
        , ap._newMargin / 1e18 AS margin
        , ap._newPrice / 1e18 AS price
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_AddToPosition') }} AS ap
    INNER JOIN
        {{ source('tigristrade_arbitrum', 'TradingV4_call_addToPosition') }} AS af
        ON ap._id = af._id
            AND ap.evt_tx_hash = af.call_tx_hash
            AND af.call_success = true
            {% if is_incremental() %}
                AND af.call_block_time >= date_trunc(
                    'day', now() - INTERVAL '1 week'
                )
            {% endif %}
            {% if is_incremental() %}
                WHERE
                    ap.evt_block_time >= date_trunc(
                        'day', now() - INTERVAL '1 week'
                    )
            {% endif %}
)

, add_margin_v5 AS (
    SELECT
        ap.evt_tx_hash
        , ap.evt_index
        , ap.evt_block_time
        , ap._id AS position_id
        , ap._trader AS trader
        , date_trunc('day', ap.evt_block_time) AS day
        , af._addMargin / 1e18 AS margin_change
        , ap._newMargin / 1e18 AS margin
        , ap._newPrice / 1e18 AS price
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV5_evt_AddToPosition') }} AS ap
    INNER JOIN
        {{ source('tigristrade_arbitrum', 'TradingV5_call_addToPosition') }} AS af
        ON ap._id = af._id
            AND ap.evt_tx_hash = af.call_tx_hash
            AND af.call_success = true
            {% if is_incremental() %}
                AND af.call_block_time >= date_trunc(
                    'day', now() - INTERVAL '1 week'
                )
            {% endif %}
            {% if is_incremental() %}
                WHERE
                    ap.evt_block_time >= date_trunc(
                        'day', now() - INTERVAL '1 week'
                    )
            {% endif %}
)


SELECT
    *
    , 'v2' AS version
FROM add_margin_v2

UNION ALL

SELECT
    *
    , 'v3' AS version
FROM add_margin_v3

UNION ALL

SELECT
    *
    , 'v4' AS version
FROM add_margin_v4

UNION ALL

SELECT
    *
    , 'v5' AS version
FROM add_margin_v5
