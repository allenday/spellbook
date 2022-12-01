{{ config(
    alias = 'transactions_gnosis_erc20',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'dao_creator_tool', 'dao', 'dao_wallet_address', 'tx_hash', 'tx_index', 'tx_type', 'trace_address', 'address_interacted_with', 'value', 'asset_contract_address']
    )
}}

{% set transactions_start_date = '2020-05-24' %}

WITH

dao_tmp AS (
        SELECT
            blockchain,
            dao_creator_tool,
            dao,
            dao_wallet_address
        FROM
        {{ ref('dao_addresses_gnosis') }}
        WHERE dao_wallet_address IS NOT NULL
),

transactions AS (
        SELECT
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            contract_address AS token,
            value AS value,
            to AS dao_wallet_address,
            'tx_in' AS tx_type,
            evt_index AS tx_index,
            FROM AS address_interacted_with,
            array('') AS trace_address
        FROM
        {{ source('erc20_gnosis', 'evt_transfer') }}
        {% if NOT is_incremental() %}
        WHERE evt_block_time >= '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND to IN (SELECT dao_wallet_address FROM dao_tmp)

        UNION ALL

        SELECT
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            contract_address AS token,
            value AS value,
            FROM AS dao_wallet_address,
            'tx_out' AS tx_type,
            evt_index AS tx_index,
            to AS address_interacted_with,
            array('') AS trace_address
        FROM
        {{ source('erc20_gnosis', 'evt_transfer') }}
        {% if NOT is_incremental() %}
        WHERE evt_block_time >= '{{transactions_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND FROM IN (SELECT dao_wallet_address FROM dao_tmp)
)

SELECT
    dt.blockchain,
    dt.dao_creator_tool,
    dt.dao,
    dt.dao_wallet_address,
    TRY_CAST(date_trunc('day', t.block_time) AS DATE) AS block_date,
    t.block_time,
    t.tx_type,
    t.token AS asset_contract_address,
    COALESCE(er.symbol, t.token) AS asset,
    t.value AS raw_value,
    t.value / POW(10, COALESCE(er.decimals, 18)) AS value,
    t.value / POW(10, COALESCE(er.decimals, 18)) * p.price AS usd_value,
    t.tx_hash,
    t.tx_index,
    t.address_interacted_with,
    t.trace_address
FROM
transactions t
INNER JOIN
dao_tmp dt
    ON t.dao_wallet_address = dt.dao_wallet_address
LEFT JOIN
{{ ref('tokens_erc20') }} er
    ON t.token = er.contract_address
    AND er.blockchain = 'gnosis'
LEFT JOIN
{{ source('prices', 'usd') }} p
    ON p.minute = date_trunc('minute', t.block_time)
    AND p.contract_address = t.token
    AND p.blockchain = 'gnosis'
    {% if NOT is_incremental() %}
    AND p.minute >= '{{transactions_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}