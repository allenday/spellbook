{{ config(
    schema = 'tigris_v2_polygon',
    alias = 'events_liquidate_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader']
    )
}}

WITH

liquidate_position_v1 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        id AS position_id,
        trader AS trader
    FROM
        {{ source('tigristrade_v2_polygon', 'Trading_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

liquidate_position_v2 AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        id AS position_id,
        trader AS trader
    FROM
        {{ source('tigristrade_v2_polygon', 'TradingV2_evt_PositionLiquidated') }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    *,
    'v2.1' AS version
FROM liquidate_position_v1

UNION ALL

SELECT
    *,
    'v2.2' AS version
FROM liquidate_position_v2
