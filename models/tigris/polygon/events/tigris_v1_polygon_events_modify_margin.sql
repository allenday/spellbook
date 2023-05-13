{{ config(
    schema = 'tigris_v1_polygon',
    alias = 'events_modify_margin',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'evt_index', 'trader', 'margin', 'leverage']
    )
}}

WITH

modify_margin_v5 AS (
    SELECT
        date_trunc('day', mm.evt_block_time) AS day,
        mm.evt_tx_hash,
        mm.evt_index,
        mm.evt_block_time,
        mm._id AS position_id,
        mm._ismarginadded AS modify_type,
        COALESCE(am._addmargin / 1e18, rm._removemargin / 1e18) AS margin_change,
        mm._newmargin / 1e18 AS margin,
        mm._newleverage / 1e18 AS leverage,
        mm._trader AS trader
    FROM
        {{ source('tigristrade_polygon', 'TradingV5_evt_MarginModified') }} AS mm
    LEFT JOIN
        {{ source('tigristrade_polygon', 'TradingV5_call_addMargin') }} AS am
        ON
            mm._id = am._id
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true
            {% if is_incremental() %}
                AND am.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    LEFT JOIN
        {{ source('tigristrade_polygon', 'TradingV5_call_removeMargin') }} AS rm
        ON
            mm._id = rm._id
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true
            {% if is_incremental() %}
                AND rm.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

modify_margin_v6 AS (
    SELECT
        date_trunc('day', mm.evt_block_time) AS day,
        mm.evt_tx_hash,
        mm.evt_index,
        mm.evt_block_time,
        mm._id AS position_id,
        mm._ismarginadded AS modify_type,
        COALESCE(am._addmargin / 1e18, rm._removemargin / 1e18) AS margin_change,
        mm._newmargin / 1e18 AS margin,
        mm._newleverage / 1e18 AS leverage,
        mm._trader AS trader
    FROM
        {{ source('tigristrade_polygon', 'TradingV6_evt_MarginModified') }} AS mm
    LEFT JOIN
        {{ source('tigristrade_polygon', 'TradingV6_call_addMargin') }} AS am
        ON
            mm._id = am._id
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true
            {% if is_incremental() %}
                AND am.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    LEFT JOIN
        {{ source('tigristrade_polygon', 'TradingV6_call_removeMargin') }} AS rm
        ON
            mm._id = rm._id
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true
            {% if is_incremental() %}
                AND rm.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

modify_margin_v7 AS (
    SELECT
        date_trunc('day', mm.evt_block_time) AS day,
        mm.evt_tx_hash,
        mm.evt_index,
        mm.evt_block_time,
        mm._id AS position_id,
        mm._ismarginadded AS modify_type,
        COALESCE(am._addmargin / 1e18, rm._removemargin / 1e18) AS margin_change,
        mm._newmargin / 1e18 AS margin,
        mm._newleverage / 1e18 AS leverage,
        mm._trader AS trader
    FROM
        {{ source('tigristrade_polygon', 'TradingV7_evt_MarginModified') }} AS mm
    LEFT JOIN
        {{ source('tigristrade_polygon', 'TradingV7_call_addMargin') }} AS am
        ON
            mm._id = am._id
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true
            {% if is_incremental() %}
                AND am.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    LEFT JOIN
        {{ source('tigristrade_polygon', 'TradingV7_call_removeMargin') }} AS rm
        ON
            mm._id = rm._id
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true
            {% if is_incremental() %}
                AND rm.call_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
    {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

modify_margin_v8 AS (
    SELECT *
    FROM
        (
            SELECT
                date_trunc('day', mm.evt_block_time) AS day,
                mm.evt_tx_hash,
                mm.evt_index,
                mm.evt_block_time,
                mm._id AS position_id,
                mm._ismarginadded AS modify_type,
                COALESCE(am._addmargin / 1e18, rm._removemargin / 1e18) AS margin_change,
                mm._newmargin / 1e18 AS margin,
                mm._newleverage / 1e18 AS leverage,
                mm._trader AS trader
            FROM
                {{ source('tigristrade_polygon', 'TradingV8_evt_MarginModified') }} AS mm
            LEFT JOIN
                {{ source('tigristrade_polygon', 'TradingV8_call_addMargin') }} AS am
                ON
                    mm._id = am._id
                    AND mm.evt_tx_hash = am.call_tx_hash
                    AND am.call_success = true
                    {% if is_incremental() %}
                        AND am.call_block_time >= date_trunc('day', now() - interval '1 week')
                    {% endif %}
            LEFT JOIN
                {{ source('tigristrade_polygon', 'TradingV8_call_removeMargin') }} AS rm
                ON
                    mm._id = rm._id
                    AND mm.evt_tx_hash = rm.call_tx_hash
                    AND rm.call_success = true
                    {% if is_incremental() %}
                        AND rm.call_block_time >= date_trunc('day', now() - interval '1 week')
                    {% endif %}
            {% if is_incremental() %}
                WHERE mm.evt_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
        ) AS t
    WHERE t.evt_tx_hash NOT IN ('0x561cde89720f8af596bf8958dd96339d8b3923094d6d27dd8bf14f5326c9ae25', '0x17e49a19c4feaf014bf485ee2277bfa09375bde9931da9a95222de7a1e704d70', '0x146e22e33c8218ac8c70502b292bbc6d9334983135a1e70ffe0125784bfdcc91')
)

SELECT
    *,
    'v1.5' AS version
FROM modify_margin_v5

UNION ALL

SELECT
    *,
    'v1.6' AS version
FROM modify_margin_v6

UNION ALL

SELECT
    *,
    'v1.7' AS version
FROM modify_margin_v7

UNION ALL

SELECT
    *,
    'v1.8' AS version
FROM modify_margin_v8;
