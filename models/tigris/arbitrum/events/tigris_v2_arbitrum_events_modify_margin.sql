{{ config(
    schema = 'tigris_v2_arbitrum',
    alias = 'events_modify_margin',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin', 'leverage']
    )
}}

WITH

modify_margin_v1 AS (
    SELECT
        date_trunc('day', mm.evt_block_time) AS day,
        mm.evt_tx_hash,
        mm.evt_index,
        mm.evt_block_time,
        mm.id AS position_id,
        mm.ismarginadded AS modify_type,
        COALESCE(am._addmargin / 1e18, rm._removemargin / 1e18) AS margin_change,
        mm.newmargin / 1e18 AS margin,
        mm.newleverage / 1e18 AS leverage,
        mm.trader AS trader
    FROM
        {{ source('tigristrade_v2_arbitrum', 'Trading_evt_MarginModified') }} AS mm
    LEFT JOIN
        {{ source('tigristrade_v2_arbitrum', 'Trading_call_addMargin') }} AS am
        ON
            mm.id = am._id
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true
            AND mm.ismarginadded = true
            {% if is_incremental() %}
                AND am.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    LEFT JOIN
        {{ source('tigristrade_v2_arbitrum', 'Trading_call_removeMargin') }} AS rm
        ON
            mm.id = rm._id
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true
            AND mm.ismarginadded = false
            {% if is_incremental() %}
                AND rm.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

modify_margin_v2 AS (
    SELECT
        date_trunc('day', mm.evt_block_time) AS day,
        mm.evt_tx_hash,
        mm.evt_index,
        mm.evt_block_time,
        mm.id AS position_id,
        mm.ismarginadded AS modify_type,
        COALESCE(am._addmargin / 1e18, rm._removemargin / 1e18) AS margin_change,
        mm.newmargin / 1e18 AS margin,
        mm.newleverage / 1e18 AS leverage,
        mm.trader AS trader
    FROM
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_evt_MarginModified') }} AS mm
    LEFT JOIN
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_call_addMargin') }} AS am
        ON
            mm.id = am._id
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true
            AND mm.ismarginadded = true
            {% if is_incremental() %}
                AND am.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    LEFT JOIN
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_call_removeMargin') }} AS rm
        ON
            mm.id = rm._id
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true
            AND mm.ismarginadded = false
            {% if is_incremental() %}
                AND rm.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    *,
    'v2.1' AS version
FROM modify_margin_v1

UNION ALL

SELECT
    *,
    'v2.2' AS version
FROM modify_margin_v2
