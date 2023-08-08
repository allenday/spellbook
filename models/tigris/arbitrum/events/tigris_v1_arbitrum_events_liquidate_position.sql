{{ config(
    schema = 'tigris_v1_arbitrum',
    alias = 'events_liquidate_position',
    partition_by = {"field": "day"},
    materialized = 'view',
            unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader']
    )
}}

WITH 

liquidate_position_v2 as (
        SELECT 
            TIMESTAMP_TRUNC(evt_block_time, day) AS `day`, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
),

liquidate_position_v3 as (
        SELECT 
            TIMESTAMP_TRUNC(evt_block_time, day) AS `day`, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
),

liquidate_position_v4 as (
        SELECT 
            TIMESTAMP_TRUNC(evt_block_time, day) AS `day`, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
),

liquidate_position_v5 as (
        SELECT 
            TIMESTAMP_TRUNC(evt_block_time, day) AS `day`, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV5_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
)

SELECT *, 'v1.2' as version FROM liquidate_position_v2

UNION ALL

SELECT *, 'v1.3' as version FROM liquidate_position_v3

UNION ALL

SELECT *, 'v1.4' as version FROM liquidate_position_v4

UNION ALL

SELECT *, 'v1.5' as version FROM liquidate_position_v5