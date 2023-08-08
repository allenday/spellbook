{{ config(materialized = 'view', alias='erc20') }}

with
    sent_transfers as (
        select
            'send-' || cast(evt_tx_hash as STRING) || '-' || cast (evt_index as STRING) || '-' || CAST(`to` AS STRING) as unique_transfer_id,
            `to` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            `value` as amount_raw
        from
            {{ source('erc20_avalanche_c', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select
        'receive-' || cast(evt_tx_hash as STRING) || '-' || cast (evt_index as STRING) || '-' || CAST("from" AS STRING) as unique_transfer_id,
        "from" as wallet_address,
        contract_address as token_address,
        evt_block_time,
        '-' || CAST(`value` AS STRING) as amount_raw
        from
            {{ source('erc20_avalanche_c', 'evt_transfer') }}
    )

    ,
    deposited_wavax as (
        select
            'deposit-' || cast(evt_tx_hash as STRING) || '-' || cast (evt_index as STRING) || '-' ||  CAST(dst AS STRING) as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            wad as amount_raw
        from
            {{ source('wavax_avalanche_c', 'wavax_evt_deposit') }}
    )

    ,
    withdrawn_wavax as (
        select
            'withdraw-' || cast(evt_tx_hash as STRING) || '-' || cast (evt_index as STRING) || '-' ||  CAST(src AS STRING) as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            '-' || CAST(wad AS STRING) as amount_raw
        from
            {{ source('wavax_avalanche_c', 'wavax_evt_withdrawal') }}
    )
    
select unique_transfer_id, 'avalanche_c' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from sent_transfers
UNION ALL
select unique_transfer_id, 'avalanche_c' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from received_transfers
UNION ALL
select unique_transfer_id, 'avalanche_c' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from deposited_wavax
UNION ALL
select unique_transfer_id, 'avalanche_c' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from withdrawn_wavax