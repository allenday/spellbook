{{ config(
    schema = 'tigris_v1_arbitrum',
    alias = 'events_liquidate_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader']
    )
}}

WITH

liquidate_position_v2 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        _id AS position_id,
        _trader AS trader
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

liquidate_position_v3 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        _id AS position_id,
        _trader AS trader
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

liquidate_position_v4 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        _id AS position_id,
        _trader AS trader
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

liquidate_position_v5 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        _id AS position_id,
        _trader AS trader
    FROM
        {{ source('tigristrade_arbitrum', 'TradingV5_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    *,
    'v1.2' AS version
FROM liquidate_position_v2

UNION ALL

SELECT
    *,
    'v1.3' AS version
FROM liquidate_position_v3

UNION ALL

SELECT
    *,
    'v1.4' AS version
FROM liquidate_position_v4

UNION ALL

SELECT
    *,
    'v1.5' AS version
FROM liquidate_position_v5;
