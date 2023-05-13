{{ config(
    schema = 'tigris_v2_polygon',
    alias = 'events_limit_order',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id']
    )
}}

WITH

pairs AS (
    SELECT *
    FROM
        {{ ref('tigris_v2_polygon_events_asset_added') }}
),

{% set limit_order_trading_evt_tables = [
    'Trading_evt_LimitOrderExecuted',
    'TradingV2_evt_LimitOrderExecuted'
] %}

limit_orders AS (
    {% for limit_order_trading_evt in limit_order_trading_evt_tables %}
        SELECT
            '{{ 'v2.' + (loop.index + 1) | string }}' AS version,
            date_trunc('day', t.evt_block_time) AS day,
            t.evt_block_time,
            t.evt_index,
            t.evt_tx_hash,
            t.id AS position_id,
            t.openprice / 1e18 AS price,
            t.margin / 1e18 AS margin,
            t.lev / 1e18 AS leverage,
            t.margin / 1e18 * t.lev / 1e18 AS volume_usd,
            '' AS margin_asset,
            ta.pair,
            CASE WHEN t.direction = true THEN 'true' ELSE 'false' END AS direction,
            '' AS referral,
            t.trader AS trader
        FROM {{ source('tigristrade_v2_polygon', limit_order_trading_evt) }} AS t
        INNER JOIN pairs AS ta
            ON t.asset = ta.asset_id
        {% if is_incremental() %}
            WHERE t.evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT *
FROM limit_orders;
