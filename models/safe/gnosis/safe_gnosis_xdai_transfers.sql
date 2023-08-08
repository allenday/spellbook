{{ 
    config(
        materialized = 'view',
        alias='xdai_transfers',
        partition_by = {"field": "block_date"},
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
                incremental_strategy='merge'
    ) 
}}

{% set project_start_date = '2020-05-21' %}

select 
    s.address,
    SAFE_CAST(TIMESTAMP_TRUNC(et.block_time, day) as date) as block_date,
    et.block_time,
    -et.value as amount_raw,
    et.tx_hash,
    STRING_AGG(et.trace_address, ',') as trace_address
from {{ source('gnosis', 'traces') }} et
join {{ ref('safe_gnosis_safes') }} s on et.from = s.address
    and et.from != et.to -- exclude calls to self to guarantee unique key property
    and et.success = true
    and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
    and et.value >0 -- value is of type string. exclude 0 value traces
{% if not is_incremental() %}
where et.block_time > '{{project_start_date}}' -- for initial query optimisation
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where et.block_time > date_trunc("day", CURRENT_TIMESTAMP() - interval '10 days')
{% endif %}
        
union all
    
select 
    s.address, 
    SAFE_CAST(TIMESTAMP_TRUNC(et.block_time, day) as date) as block_date,
    et.block_time,
    et.value as amount_raw,
    et.tx_hash,
    STRING_AGG(et.trace_address, ',') as trace_address
from {{ source('gnosis', 'traces') }} et
join {{ ref('safe_gnosis_safes') }} s on et.to = s.address
    and et.from != et.to -- exclude calls to self to guarantee unique key property
    and et.success = true
    and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
    and et.value >0 -- value is of type string. exclude 0 value traces
{% if not is_incremental() %}
where et.block_time > '{{project_start_date}}' -- for initial query optimisation
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where et.block_time > date_trunc("day", CURRENT_TIMESTAMP() - interval '10 days')
{% endif %}

union all

select 
    s.address, 
    SAFE_CAST(TIMESTAMP_TRUNC(a.evt_block_time, day) as date) as block_date,
    a.evt_block_time as block_time, 
    a.amount as amount_raw,
    a.evt_tx_hash as tx_hash,
    cast(array(a.evt_index) as string) as trace_address
from {{ source('xdai_gnosis', 'BlockRewardAuRa_evt_AddedReceiver') }} a
join {{ ref('safe_gnosis_safes') }} s
    on a.receiver = s.address
{% if not is_incremental() %}
where a.evt_block_time > '{{project_start_date}}' -- for initial query optimisation
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where a.evt_block_time > date_trunc("day", CURRENT_TIMESTAMP() - interval '10 days')
{% endif %}