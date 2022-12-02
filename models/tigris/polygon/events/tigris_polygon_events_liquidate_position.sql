{{ config(
    alias = 'events_liquidate_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'evt_index']
    )
}}

WITH

liquidate_position_v1 AS (
    SELECT
        pl.evt_tx_hash
        , pl.evt_index
        , pl.evt_block_time
        , pl._id AS position_id
        , op.trader AS trader
        , date_trunc('day', pl.evt_block_time) AS day
    FROM
        {{ source('tigristrade_polygon', 'Tradingv1_evt_PositionLiquidated') }} AS pl
    INNER JOIN
        {{ ref('tigris_polygon_events_open_position') }} AS op
        ON pl._id = op.position_id
            AND op.version = 'v1'
    {% if is_incremental() %}
        WHERE pl.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, liquidate_position_v2 AS (
    SELECT
        pl.evt_tx_hash
        , pl.evt_index
        , pl.evt_block_time
        , pl._id AS position_id
        , op.trader AS trader
        , date_trunc('day', pl.evt_block_time) AS day
    FROM
        {{ source('tigristrade_polygon', 'TradingV2_evt_PositionLiquidated') }} AS pl
    INNER JOIN
        {{ ref('tigris_polygon_events_open_position') }} AS op
        ON pl._id = op.position_id
            AND op.version = 'v2'
    {% if is_incremental() %}
        WHERE pl.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, liquidate_position_v3 AS (
    SELECT
        pl.evt_tx_hash
        , pl.evt_index
        , pl.evt_block_time
        , pl._id AS position_id
        , op.trader AS trader
        , date_trunc('day', pl.evt_block_time) AS day
    FROM
        {{ source('tigristrade_polygon', 'TradingV3_evt_PositionLiquidated') }} AS pl
    INNER JOIN
        {{ ref('tigris_polygon_events_open_position') }} AS op
        ON pl._id = op.position_id
            AND op.version = 'v3'
    {% if is_incremental() %}
        WHERE pl.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, liquidate_position_v4 AS (
    SELECT
        pl.evt_tx_hash
        , pl.evt_index
        , pl.evt_block_time
        , pl._id AS position_id
        , op.trader AS trader
        , date_trunc('day', pl.evt_block_time) AS day
    FROM
        {{ source('tigristrade_polygon', 'TradingV4_evt_PositionLiquidated') }} AS pl
    INNER JOIN
        {{ ref('tigris_polygon_events_open_position') }} AS op
        ON pl._id = op.position_id
            AND op.version = 'v4'
    {% if is_incremental() %}
        WHERE pl.evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, liquidate_position_v5 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
    FROM
        {{ source('tigristrade_polygon', 'TradingV5_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, liquidate_position_v6 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
    FROM
        {{ source('tigristrade_polygon', 'TradingV6_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, liquidate_position_v7 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
    FROM
        {{ source('tigristrade_polygon', 'TradingV7_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

, liquidate_position_v8 AS (
    SELECT
        evt_tx_hash
        , evt_index
        , evt_block_time
        , _id AS position_id
        , _trader AS trader
        , date_trunc('day', evt_block_time) AS day
    FROM
        {{ source('tigristrade_polygon', 'TradingV8_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
    {% endif %}
)

SELECT
    *
    , 'v1' AS version
FROM liquidate_position_v1

UNION ALL

SELECT
    *
    , 'v2' AS version
FROM liquidate_position_v2

UNION ALL

SELECT
    *
    , 'v3' AS version
FROM liquidate_position_v3

UNION ALL

SELECT
    *
    , 'v4' AS version
FROM liquidate_position_v4

UNION ALL

SELECT
    *
    , 'v5' AS version
FROM liquidate_position_v5

UNION ALL

SELECT
    *
    , 'v6' AS version
FROM liquidate_position_v6

UNION ALL

SELECT
    *
    , 'v7' AS version
FROM liquidate_position_v7

UNION ALL

SELECT
    *
    , 'v8' AS version
FROM liquidate_position_v8
