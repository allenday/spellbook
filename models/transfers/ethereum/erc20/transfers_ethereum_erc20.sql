{{ config(materialized = 'view', alias='erc20') }}

with
    sent_transfers as (
        select
            CAST('send' AS STRING) || CAST('-' AS STRING) || CAST(evt_tx_hash AS STRING) || CAST('-' AS STRING) || CAST(evt_index AS STRING) || CAST('-' AS STRING) || CAST(`to` AS STRING) as unique_transfer_id,
            `to` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            `value` as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    received_transfers as (
        select
        CAST('receive' AS STRING) || CAST('-' AS STRING) || CAST(evt_tx_hash AS STRING) || CAST('-' AS STRING) || CAST(evt_index AS STRING) || CAST('-' AS STRING) || CAST(`from` AS STRING) as unique_transfer_id,
        `from` as wallet_address,
        contract_address as token_address,
        evt_block_time,
        '-' || CAST(`value` AS STRING) as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    deposited_weth as (
        select
            CAST('deposit' AS STRING) || CAST('-' AS STRING) || CAST(evt_tx_hash AS STRING) || CAST('-' AS STRING) || CAST(evt_index AS STRING) || CAST('-' AS STRING) || CAST(dst AS STRING) as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            wad as amount_raw
        from
            {{ source('zeroex_ethereum', 'weth9_evt_Deposit') }}
    )

    ,
    withdrawn_weth as (
        select
            CAST('withdrawn' AS STRING) || CAST('-' AS STRING) || CAST(evt_tx_hash AS STRING) || CAST('-' AS STRING) || CAST(evt_index AS STRING) || CAST('-' AS STRING) || CAST(src AS STRING) as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            '-' || CAST(wad AS STRING) as amount_raw
        from
            {{ source('zeroex_ethereum', 'weth9_evt_Withdrawal') }}
    )
    
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from sent_transfers
UNION ALL
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from received_transfers
UNION ALL
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from deposited_weth
UNION ALL
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from withdrawn_weth