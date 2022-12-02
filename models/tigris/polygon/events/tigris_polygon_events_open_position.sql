{{ config(
    alias = 'events_open_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_index', 'evt_tx_hash', 'position_id']
    )
}}

WITH

pairs AS (
    SELECT *
    FROM
        {{ ref('tigris_polygon_events_asset_added') }}
)

, open_positions_v1 AS (
    SELECT
        t.evt_block_time
        , t.evt_index
        , t.evt_tx_hash
        , t._id AS position_id
        , t._tradeInfo:marginAsset AS margin_asset
        , pairs.pair
        , t._tradeInfo:direction AS direction
        , t._tradeInfo:referral AS referral
        , t._trader AS trader
        , date_trunc('day', t.evt_block_time) AS day
        , t._price / 1e18 AS price
        , t._tradeInfo:margin / 1e18 AS margin
        , t._tradeInfo:leverage / 1e18 AS leverage
        , t._tradeInfo:margin / 1e18 * _tradeInfo:leverage / 1e18 AS volume_usd
    FROM
        {{ source('tigristrade_polygon', 'Tradingv1_evt_PositionOpened') }} AS t
    INNER JOIN
        pairs
        ON t._tradeInfo:asset = pairs.asset_id
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, open_positions_v2 AS (
    SELECT
        t.evt_block_time
        , t.evt_index
        , t.evt_tx_hash
        , t._id AS position_id
        , t._tradeInfo:marginAsset AS margin_asset
        , pairs.pair
        , t._tradeInfo:direction AS direction
        , t._tradeInfo:referral AS referral
        , t._trader AS trader
        , date_trunc('day', t.evt_block_time) AS day
        , t._price / 1e18 AS price
        , t._tradeInfo:margin / 1e18 AS margin
        , t._tradeInfo:leverage / 1e18 AS leverage
        , t._tradeInfo:margin / 1e18 * _tradeInfo:leverage / 1e18 AS volume_usd
    FROM
        {{ source('tigristrade_polygon', 'TradingV2_evt_PositionOpened') }} AS t
    INNER JOIN
        pairs
        ON t._tradeInfo:asset = pairs.asset_id
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, open_positions_v3 AS (
    SELECT
        t.evt_block_time
        , t.evt_index
        , t.evt_tx_hash
        , t._id AS position_id
        , t._tradeInfo:marginAsset AS margin_asset
        , pairs.pair
        , t._tradeInfo:direction AS direction
        , t._tradeInfo:referral AS referral
        , t._trader AS trader
        , date_trunc('day', t.evt_block_time) AS day
        , t._price / 1e18 AS price
        , t._tradeInfo:margin / 1e18 AS margin
        , t._tradeInfo:leverage / 1e18 AS leverage
        , t._tradeInfo:margin / 1e18 * _tradeInfo:leverage / 1e18 AS volume_usd
    FROM
        {{ source('tigristrade_polygon', 'TradingV3_evt_PositionOpened') }} AS t
    INNER JOIN
        pairs
        ON t._tradeInfo:asset = pairs.asset_id
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, open_positions_v4 AS (
    SELECT
        t.evt_block_time
        , t.evt_index
        , t.evt_tx_hash
        , t._id AS position_id
        , t._tradeInfo:marginAsset AS margin_asset
        , pairs.pair
        , t._tradeInfo:direction AS direction
        , t._tradeInfo:referral AS referral
        , t._trader AS trader
        , date_trunc('day', t.evt_block_time) AS day
        , t._price / 1e18 AS price
        , t._tradeInfo:margin / 1e18 AS margin
        , t._tradeInfo:leverage / 1e18 AS leverage
        , t._tradeInfo:margin / 1e18 * _tradeInfo:leverage / 1e18 AS volume_usd
    FROM
        {{ source('tigristrade_polygon', 'TradingV4_evt_PositionOpened') }} AS t
    INNER JOIN
        pairs
        ON t._tradeInfo:asset = pairs.asset_id
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, open_positions_v5 AS (
    SELECT
        t.evt_block_time
        , t.evt_index
        , t.evt_tx_hash
        , t._id AS position_id
        , t._tradeInfo:marginAsset AS margin_asset
        , pairs.pair
        , t._tradeInfo:direction AS direction
        , t._tradeInfo:referral AS referral
        , t._trader AS trader
        , date_trunc('day', t.evt_block_time) AS day
        , t._price / 1e18 AS price
        , t._tradeInfo:margin / 1e18 AS margin
        , t._tradeInfo:leverage / 1e18 AS leverage
        , t._tradeInfo:margin / 1e18 * _tradeInfo:leverage / 1e18 AS volume_usd
    FROM
        {{ source('tigristrade_polygon', 'TradingV5_evt_PositionOpened') }} AS t
    INNER JOIN
        pairs
        ON t._tradeInfo:asset = pairs.asset_id
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, open_positions_v6 AS (
    SELECT
        t.evt_block_time
        , t.evt_index
        , t.evt_tx_hash
        , t._id AS position_id
        , t._tradeInfo:marginAsset AS margin_asset
        , pairs.pair
        , t._tradeInfo:direction AS direction
        , t._tradeInfo:referral AS referral
        , t._trader AS trader
        , date_trunc('day', t.evt_block_time) AS day
        , t._price / 1e18 AS price
        , t._tradeInfo:margin / 1e18 AS margin
        , t._tradeInfo:leverage / 1e18 AS leverage
        , t._tradeInfo:margin / 1e18 * _tradeInfo:leverage / 1e18 AS volume_usd
    FROM
        {{ source('tigristrade_polygon', 'TradingV6_evt_PositionOpened') }} AS t
    INNER JOIN
        pairs
        ON t._tradeInfo:asset = pairs.asset_id
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, open_positions_v7 AS (
    SELECT
        t.evt_block_time
        , t.evt_index
        , t.evt_tx_hash
        , t._id AS position_id
        , t._tradeInfo:marginAsset AS margin_asset
        , pairs.pair
        , t._tradeInfo:direction AS direction
        , t._tradeInfo:referral AS referral
        , t._trader AS trader
        , date_trunc('day', t.evt_block_time) AS day
        , t._price / 1e18 AS price
        , t._tradeInfo:margin / 1e18 AS margin
        , t._tradeInfo:leverage / 1e18 AS leverage
        , t._tradeInfo:margin / 1e18 * _tradeInfo:leverage / 1e18 AS volume_usd
    FROM
        {{ source('tigristrade_polygon', 'TradingV7_evt_PositionOpened') }} AS t
    INNER JOIN
        pairs
        ON t._tradeInfo:asset = pairs.asset_id
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, open_positions_v8 AS (
    SELECT
        t.evt_block_time
        , t.evt_index
        , t.evt_tx_hash
        , t._id AS position_id
        , t._tradeInfo:marginAsset AS margin_asset
        , pairs.pair
        , t._tradeInfo:direction AS direction
        , t._tradeInfo:referral AS referral
        , t._trader AS trader
        , date_trunc('day', t.evt_block_time) AS day
        , t._price / 1e18 AS price
        , t._tradeInfo:margin / 1e18 AS margin
        , t._tradeInfo:leverage / 1e18 AS leverage
        , t._tradeInfo:margin / 1e18 * _tradeInfo:leverage / 1e18 AS volume_usd
    FROM
        {{ source('tigristrade_polygon', 'TradingV8_evt_PositionOpened') }} AS t
    INNER JOIN
        pairs
        ON t._tradeInfo:asset = pairs.asset_id
    {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

SELECT
    *
    , 'v1' AS version
FROM open_positions_v1

UNION ALL

SELECT
    *
    , 'v2' AS version
FROM open_positions_v2

UNION ALL

SELECT
    *
    , 'v3' AS version
FROM open_positions_v3

UNION ALL

SELECT
    *
    , 'v4' AS version
FROM open_positions_v4

UNION ALL

SELECT
    *
    , 'v5' AS version
FROM open_positions_v5

UNION ALL

SELECT
    *
    , 'v6' AS version
FROM open_positions_v6

UNION ALL

SELECT
    *
    , 'v7' AS version
FROM open_positions_v7

UNION ALL

SELECT
    *
    , 'v8' AS version
FROM open_positions_v8
