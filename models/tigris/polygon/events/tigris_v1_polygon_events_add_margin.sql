{{ config(
    schema = 'tigris_v1_polygon',
    alias = 'events_add_margin',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin', 'evt_index']
    )
}}

WITH

add_margin_v5 AS (
    SELECT
        date_trunc('day', ap.evt_block_time) AS day,
        ap.evt_tx_hash,
        ap.evt_index,
        ap.evt_block_time,
        ap._id AS position_id,
        af._addmargin / 1e18 AS margin_change,
        ap._newmargin / 1e18 AS margin,
        ap._newprice / 1e18 AS price,
        ap._trader AS trader
    FROM
        {{ source('tigristrade_polygon', 'TradingV5_evt_AddToPosition') }} AS ap
    INNER JOIN
        {{ source('tigristrade_polygon', 'TradingV5_call_addToPosition') }} AS af
        ON
            ap._id = af._id
            AND ap.evt_tx_hash = af.call_tx_hash
            AND af.call_success = true
            {% if is_incremental() %}
                AND af.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

add_margin_v6 AS (
    SELECT
        date_trunc('day', ap.evt_block_time) AS day,
        ap.evt_tx_hash,
        ap.evt_index,
        ap.evt_block_time,
        ap._id AS position_id,
        af._addmargin / 1e18 AS margin_change,
        ap._newmargin / 1e18 AS margin,
        ap._newprice / 1e18 AS price,
        ap._trader AS trader
    FROM
        {{ source('tigristrade_polygon', 'TradingV6_evt_AddToPosition') }} AS ap
    INNER JOIN
        {{ source('tigristrade_polygon', 'TradingV6_call_addToPosition') }} AS af
        ON
            ap._id = af._id
            AND ap.evt_tx_hash = af.call_tx_hash
            AND af.call_success = true
            {% if is_incremental() %}
                AND af.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %} 
    {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

add_margin_v7 AS (
    SELECT
        date_trunc('day', ap.evt_block_time) AS day,
        ap.evt_tx_hash,
        ap.evt_index,
        ap.evt_block_time,
        ap._id AS position_id,
        af._addmargin / 1e18 AS margin_change,
        ap._newmargin / 1e18 AS margin,
        ap._newprice / 1e18 AS price,
        ap._trader AS trader
    FROM
        {{ source('tigristrade_polygon', 'TradingV7_evt_AddToPosition') }} AS ap
    INNER JOIN
        {{ source('tigristrade_polygon', 'TradingV7_call_addToPosition') }} AS af
        ON
            ap._id = af._id
            AND ap.evt_tx_hash = af.call_tx_hash
            AND af.call_success = true
            {% if is_incremental() %}
                AND af.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

add_margin_v8 AS (
    SELECT
        date_trunc('day', ap.evt_block_time) AS day,
        ap.evt_tx_hash,
        ap.evt_index,
        ap.evt_block_time,
        ap._id AS position_id,
        af._addmargin / 1e18 AS margin_change,
        ap._newmargin / 1e18 AS margin,
        ap._newprice / 1e18 AS price,
        ap._trader AS trader
    FROM
        {{ source('tigristrade_polygon', 'TradingV8_evt_AddToPosition') }} AS ap
    INNER JOIN
        {{ source('tigristrade_polygon', 'TradingV8_call_addToPosition') }} AS af
        ON
            ap._id = af._id
            AND ap.evt_tx_hash = af.call_tx_hash
            AND af.call_success = true
            {% if is_incremental() %}
                AND af.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    *,
    'v1.5' AS version
FROM add_margin_v5

UNION ALL

SELECT
    *,
    'v1.6' AS version
FROM add_margin_v6

UNION ALL

SELECT
    *,
    'v1.7' AS version
FROM add_margin_v7

UNION ALL

SELECT
    *,
    'v1.8' AS version
FROM add_margin_v8;
