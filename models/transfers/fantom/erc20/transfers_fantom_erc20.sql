{{ config(
    alias='erc20',
    materialized = 'view',
            unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address']
    )
}}

with
    sent_transfers as (
        select 
            'send'as transfer_type,
            evt_tx_hash,
            evt_index,
            et.to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            `value` as amount_raw
        from
            {{ source('erc20_fantom', 'evt_transfer') }} et
        {% if is_incremental() %}
            where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
    ),
    received_transfers as (
        select
            'receive'as transfer_type,
            evt_tx_hash,
            evt_index,
            et.from as wallet_address,
            contract_address as token_address,
            evt_block_time,
            '-' || CAST(`value` AS STRING) as amount_raw
        from
            {{ source('erc20_fantom', 'evt_transfer') }} et
        {% if is_incremental() %}
            where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
    )

-- There is no need to add wrapped FTM deposits / withdrawals since wrapped FTM on fantom triggers transfer events for both.
    
select 
    transfer_type,
    'fantom' as blockchain, 
    evt_tx_hash,
    evt_index,
    wallet_address,
    token_address,
    evt_block_time,
    CAST(amount_raw AS STRING) as amount_raw
from sent_transfers

UNION ALL

select 
    transfer_type,
    'fantom' as blockchain, 
    evt_tx_hash,
    evt_index,
    wallet_address,
    token_address,
    evt_block_time, 
    CAST(amount_raw AS STRING) as amount_raw
from received_transfers