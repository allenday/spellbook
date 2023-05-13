{{ config(
    alias = 'perpetual_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "emdx",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-05-21' %}

WITH

perp_events AS (
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        CASE WHEN (exchangedpositionsize * 1) >= 0 THEN 'long' ELSE 'short' END AS trade_type,
        '' AS virtual_asset,    -- cant find in events
        '' AS underlying_asset, -- cant find in events
        positionnotional / 1E18 AS volume_usd,
        fee / 1E18 AS fee_usd,
        margin / 1E18 AS margin_usd,
        CAST(positionnotional AS double) AS volume_raw,
        trader,
        amm AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('emdx_avalanche_c', 'ClearingHouse_evt_PositionChanged') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

trade_data AS (
    -- close position calls 
    SELECT
        call_block_number AS block_number,
        call_tx_hash AS tx_hash,
        'close' AS trade_data
    FROM
        {{ source('emdx_avalanche_c', 'ClearingHouse_call_closePosition') }}
    WHERE
        call_success = true
        {% if not is_incremental() %}
    AND call_block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

    UNION ALL

    -- open position calls 
    SELECT
        call_block_number AS block_number,
        call_tx_hash AS tx_hash,
        'open' AS trade_data
    FROM
        {{ source('emdx_avalanche_c', 'ClearingHouse_call_openPosition') }}
    WHERE
        call_success = true
        {% if not is_incremental() %}
    AND call_block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

    UNION ALL

    -- liquidate position events
    SELECT
        evt_block_number AS block_number,
        evt_tx_hash AS tx_hash,
        'liquidate' AS trade_data
    FROM
        {{ source('emdx_avalanche_c', 'ClearingHouse_evt_PositionLiquidated') }}
    WHERE
        1 = 1
        {% if not is_incremental() %}
    AND evt_block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
)

SELECT
    'avalanche_c' AS blockchain,
    'emdx' AS project,
    '1' AS version,
    'emdx' AS frontend,
    date_trunc('day', pe.block_time) AS block_date,
    pe.block_time,
    pe.virtual_asset,
    pe.underlying_asset,
    '' AS market, -- no event emitted to get data
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    COALESCE(
        td.trade_data || '-' || pe.trade_type, -- using the call/open functions to classify trades
        'adjust' || '-' || pe.trade_type
    ) AS trade,
    pe.trader,
    pe.volume_raw,
    pe.tx_hash,
    txns.to AS tx_to,
    txns.from AS tx_from,
    pe.evt_index
FROM
    perp_events AS pe
INNER JOIN
    {{ source('avalanche_c', 'transactions') }} AS txns
    ON
        pe.tx_hash = txns.hash
        AND pe.block_number = txns.block_number
        {% if not is_incremental() %}
    AND txns.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND txns.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN
    trade_data AS td
    ON
        pe.block_number = td.block_number
        AND pe.tx_hash = td.tx_hash
