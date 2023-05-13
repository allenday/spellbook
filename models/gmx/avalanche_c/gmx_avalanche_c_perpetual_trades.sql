{{ config(
    alias = 'perpetual_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "gmx",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2021-12-22' %}

WITH

perp_events AS (
    -- decrease position
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'decrease_position' AS trade_data,
        indextoken AS virtual_asset,
        collateraltoken AS underlying_asset,
        sizedelta / 1E30 AS volume_usd,
        fee / 1E30 AS fee_usd,
        collateraldelta / 1E30 AS margin_usd,
        CAST(sizedelta AS double) AS volume_raw,
        CASE WHEN islong = false THEN 'short' ELSE 'long' END AS trade_type,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('gmx_avalanche_c', 'Vault_evt_DecreasePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    -- increase position  
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'increase_position' AS trade_data,
        indextoken AS virtual_asset,
        collateraltoken AS underlying_asset,
        sizedelta / 1E30 AS volume_usd,
        fee / 1E30 AS fee_usd,
        collateraldelta / 1E30 AS margin_usd,
        CAST(sizedelta AS double) AS volume_raw,
        CASE WHEN islong = false THEN 'short' ELSE 'long' END AS trade_type,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('gmx_avalanche_c', 'Vault_evt_IncreasePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    -- liquidate position 
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'liquidate_position' AS trade_data,
        indextoken AS virtual_asset,
        collateraltoken AS underlying_asset,
        size / 1E30 AS volume_usd,
        0 AS fee_usd,
        collateral / 1E30 AS margin_usd,
        CAST(size AS double) AS volume_raw,
        CASE WHEN islong = false THEN 'short' ELSE 'long' END AS trade_type,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('gmx_avalanche_c', 'Vault_evt_LiquidatePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)

SELECT
    'avalanche_c' AS blockchain,
    'gmx' AS project,
    '1' AS version,
    'gmx' AS frontend,
    date_trunc('day', pe.block_time) AS block_date,
    pe.block_time,
    COALESCE(erc20a.symbol, pe.virtual_asset) AS virtual_asset,
    COALESCE(erc20b.symbol, pe.underlying_asset) AS underlying_asset,
    CASE
        WHEN pe.virtual_asset = pe.underlying_asset THEN COALESCE(erc20a.symbol, pe.virtual_asset)
        ELSE COALESCE(erc20a.symbol, pe.virtual_asset) || '-' || COALESCE(erc20b.symbol, pe.underlying_asset)
    END AS market,
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    CASE
        WHEN pe.trade_data = 'increase_position' THEN 'open' || '-' || pe.trade_type
        WHEN pe.trade_data = 'decrease_position' THEN 'close' || '-' || pe.trade_type
        WHEN pe.trade_data = 'liquidate_position' THEN 'liquidate' || '-' || pe.trade_type
    END AS trade,
    pe.trader,
    pe.volume_raw,
    pe.tx_hash,
    txns.to AS tx_to,
    txns.from AS tx_from,
    pe.evt_index
FROM
    perp_events AS pe
INNER JOIN {{ source('avalanche_c', 'transactions') }} AS txns
    ON
        pe.tx_hash = txns.hash
        AND pe.block_number = txns.block_number
        {% if not is_incremental() %}
    AND txns.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND txns.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20a
    ON
        erc20a.contract_address = pe.virtual_asset
        AND erc20a.blockchain = 'avalanche_c'
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20b
    ON
        erc20b.contract_address = pe.underlying_asset
        AND erc20b.blockchain = 'avalanche_c'
