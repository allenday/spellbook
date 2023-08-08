{{
    config(
        schema = 'balancer_v2_optimism',
        alias='transfers_bpt',
        partition_by = {"field": "block_date"},
        materialized = 'view',
                        unique_key = ['block_date', 'evt_tx_hash', 'evt_index']
    )
}}

{% set event_signature = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' %}
{% set project_start_date = '2022-05-19' %}

WITH registered_pools AS (
    SELECT
      DISTINCT poolAddress AS pool_address
    FROM
      {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= DATE_TRUNC('day', CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %} 
  )

SELECT DISTINCT * FROM (
    SELECT
        logs.contract_address,
        logs.tx_hash AS evt_tx_hash,
        logs.index AS evt_index,
        logs.block_time AS evt_block_time,
        SAFE_CAST(TIMESTAMP_TRUNC(logs.block_time, DAY) AS date) AS block_date,
        logs.block_number AS evt_block_number,
        CONCAT('0x', SUBSTRING(logs.topic1, 27, 40)) AS `from`,
        CONCAT('0x', SUBSTRING(logs.topic2, 27, 40)) AS `to`,
        udfs.bytea2numeric(SUBSTRING(logs.data, 32, 64)) AS `value`
    FROM {{ source('optimism', 'logs') }} logs
    INNER JOIN registered_pools p ON p.pool_address = logs.contract_address
    WHERE logs.topic1 IS NULL AND logs.topic1 = '{{ event_signature }}'
        {% if not is_incremental() %}
        AND logs.block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        AND logs.block_time >= DATE_TRUNC('day', CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %} ) transfers