{{
    config(
        alias ='eth',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}
with eth_transfers AS (
    SELECT
        r.from
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000') AS contract_address
        ,r.value
        ,r.value / 1e18 AS value_decimal
        ,r.tx_hash
        ,r.trace_address
        ,r.block_time AS tx_block_time
        ,r.block_number AS tx_block_number
        ,substring(t.data, 1, 10) AS tx_method_id
        ,r.tx_hash || '-' || r.trace_address::string AS unique_transfer_id
    from {{ source('optimism', 'traces') }} AS r
    join {{ source('optimism', 'transactions') }} AS t
        on r.tx_hash = t.hash
    where
        (r.call_type NOT in ('delegatecall', 'callcode', 'staticcall') or r.call_type is NULL)
        and r.tx_success
        and r.success
        and r.value > 0
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        and r.block_time >= date_trunc('day', now() - interval '1 week')
        and t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

    union all
    --ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do NOT appear in traces.

    SELECT
        r.from
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000') AS contract_address
        ,r.value
        ,r.value / 1e18 AS value_decimal
        ,r.evt_tx_hash AS tx_hash
        ,array(r.evt_index) AS trace_address
        ,r.evt_block_time AS tx_block_time
        ,r.evt_block_number AS tx_block_number
        ,substring(t.data, 1, 10) AS tx_method_id
        ,r.evt_tx_hash || '-' || array(r.evt_index)::string AS unique_transfer_id
    from {{ source('erc20_optimism', 'evt_transfer') }} AS r
    join {{ source('optimism', 'transactions') }} AS t
        on r.evt_tx_hash = t.hash
    where
        r.contract_address = lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000')
        and t.success
        and r.value > 0
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        and r.evt_block_time >= date_trunc('day', now() - interval '1 week')
        and t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
)
SELECT * from eth_transfers order by tx_block_time
