{{
    config(
        materialized='incremental',
        alias='eth_transfers',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'tx_hash', 'trace_address', 'amount_raw'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "safe",
                                    \'["sche", "tschubotz"]\') }}'
    )
}}

SELECT
    s.address,
    try_cast(date_trunc('day', et.block_time) AS date) AS block_date,
    et.block_time,
    -et.value AS amount_raw,
    et.tx_hash,
    array_join(et.trace_address, ', ') AS trace_address
FROM {{ source('ethereum', 'traces') }} et
join {{ ref('safe_ethereum_safes') }} s ON et.from = s.address
    AND et.from != et.to -- exclude calls to self to guarantee unique key property
    AND et.success = true
    AND (lower(et.call_type) NOT in ('delegatecall', 'callcode', 'staticcall') or et.call_type is NULL)
    AND cast(et.value AS decimal(38, 0)) > 0 -- value is of type STRING. exclude 0 value traces
{% if NOT is_incremental() %}
where et.block_time > '2018-11-24' -- for initial query optimisation
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where et.block_time > date_trunc("day", now() - interval '10 days')
{% endif %}

union all

SELECT
    s.address,
    try_cast(date_trunc('day', et.block_time) AS date) AS block_date,
    et.block_time,
    et.value AS amount_raw,
    et.tx_hash,
    array_join(et.trace_address, ', ') AS trace_address
FROM {{ source('ethereum', 'traces') }} et
join {{ ref('safe_ethereum_safes') }} s ON et.to = s.address
    AND et.from != et.to -- exclude calls to self to guarantee unique key property
    AND et.success = true
    AND (lower(et.call_type) NOT in ('delegatecall', 'callcode', 'staticcall') or et.call_type is NULL)
    AND cast(et.value AS decimal(38, 0)) > 0 -- value is of type STRING. exclude 0 value traces
{% if NOT is_incremental() %}
where et.block_time > '2018-11-24' -- for initial query optimisation
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where et.block_time > date_trunc("day", now() - interval '10 days')
{% endif %}