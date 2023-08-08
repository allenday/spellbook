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
            {{ source('erc20_arbitrum', 'evt_transfer') }}
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
            {{ source('erc20_arbitrum', 'evt_transfer') }}
    )

-- There is no need to add WETH deposits / withdrawals since WETH on Arbitrum triggers transfer evens for both.
    
select unique_transfer_id, 'arbitrum' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from sent_transfers
UNION ALL
select unique_transfer_id, 'arbitrum' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS STRING) as amount_raw
from received_transfers