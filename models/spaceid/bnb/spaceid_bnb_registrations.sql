{{
    config(
        alias='registrations'
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['name']
        ,post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "spaceid",
                                    \'["springzh"]\') }}'
    )
}}
SELECT
    'v3' AS version,
    evt_block_time AS block_time,
    name,
    label,
    owner,
    cast(cost AS double) AS cost,
    cast(expires AS bigint) AS expires,
    contract_address,
    evt_tx_hash AS tx_hash,
    evt_block_number AS block_number,
    evt_index
FROM {{ source('spaceid_bnb', 'BNBRegistrarControllerV3_evt_NameRegistered') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

UNION ALL

SELECT
    'v4' AS version,
    evt_block_time AS block_time,
    name,
    label,
    owner,
    cast(cost AS double) AS cost,
    cast(expires AS bigint) AS expires,
    contract_address,
    evt_tx_hash AS tx_hash,
    evt_block_number AS block_number,
    evt_index
FROM {{ source('spaceid_bnb', 'BNBRegistrarControllerV4_evt_NameRegistered') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

UNION ALL

SELECT
    'v5' AS version,
    evt_block_time AS block_time,
    name,
    label,
    owner,
    cast(cost AS double) AS cost,
    cast(expires AS bigint) AS expires,
    contract_address,
    evt_tx_hash AS tx_hash,
    evt_block_number AS block_number,
    evt_index
FROM {{ source('spaceid_bnb', 'BNBRegistrarControllerV5_evt_NameRegistered') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

UNION ALL

SELECT
    'v6' AS version,
    evt_block_time AS block_time,
    name,
    label,
    owner,
    cast(cost AS double) AS cost,
    cast(expires AS bigint) AS expires,
    contract_address,
    evt_tx_hash AS tx_hash,
    evt_block_number AS block_number,
    evt_index
FROM {{ source('spaceid_bnb', 'BNBRegistrarControllerV6_evt_NameRegistered') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

UNION ALL

SELECT
    'v7' AS version,
    evt_block_time AS block_time,
    name,
    label,
    owner,
    cast(cost AS double) AS cost,
    cast(expires AS bigint) AS expires,
    contract_address,
    evt_tx_hash AS tx_hash,
    evt_block_number AS block_number,
    evt_index
FROM {{ source('spaceid_bnb', 'BNBRegistrarControllerV7_evt_NameRegistered') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

UNION ALL

SELECT
    'v8' AS version,
    evt_block_time AS block_time,
    name,
    label,
    owner,
    cast(cost AS double) AS cost,
    cast(expires AS bigint) AS expires,
    contract_address,
    evt_tx_hash AS tx_hash,
    evt_block_number AS block_number,
    evt_index
FROM {{ source('spaceid_bnb', 'BNBRegistrarControllerV8_evt_NameRegistered') }}
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}

UNION ALL

-- There are some records in v9 table are duplicated with those in v5 table. So we join to exclude them
SELECT
    'v9' AS version,
    v9.evt_block_time AS block_time,
    v9.name,
    v9.label,
    v9.owner,
    cast(v9.cost AS double) AS cost,
    cast(v9.expires AS bigint) AS expires,
    v9.contract_address,
    v9.evt_tx_hash AS tx_hash,
    v9.evt_block_number AS block_number,
    v9.evt_index
FROM {{ source('spaceid_bnb', 'BNBRegistrarControllerV9_evt_NameRegistered') }} AS v9
LEFT JOIN {{ source('spaceid_bnb', 'BNBRegistrarControllerV5_evt_NameRegistered') }} AS v5
    ON v9.name = v5.name
WHERE
    v5.name IS NULL
    {% if is_incremental() %}
        AND v9.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
