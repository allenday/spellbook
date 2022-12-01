{{ config(materialized='view', alias='erc20',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat"]\') }}') }}

with
    sent_transfers AS (
        SELECT
            'send' || '-' || evt_tx_hash || '-' || evt_index || '-' || `to` AS unique_transfer_id,
            `to` AS wallet_address,
            contract_address AS token_address,
            evt_block_time,
            value AS amount_raw
        FROM
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    received_transfers AS (
        SELECT
        'receive' || '-' || evt_tx_hash || '-' || evt_index || '-' || `FROM` AS unique_transfer_id,
        `FROM` AS wallet_address,
        contract_address AS token_address,
        evt_block_time,
        - value AS amount_raw
        FROM
            {{ source('erc20_ethereum', 'evt_transfer') }}
    )

    ,
    deposited_weth AS (
        SELECT
            'deposit' || '-' || evt_tx_hash || '-' || evt_index || '-' || dst AS unique_transfer_id,
            dst AS wallet_address,
            contract_address AS token_address,
            evt_block_time,
            wad AS amount_raw
        FROM
            {{ source('zeroex_ethereum', 'weth9_evt_deposit') }}
    )

    ,
    withdrawn_weth AS (
        SELECT
            'withdrawn' || '-' || evt_tx_hash || '-' || evt_index || '-' || src AS unique_transfer_id,
            src AS wallet_address,
            contract_address AS token_address,
            evt_block_time,
            - wad AS amount_raw
        FROM
            {{ source('zeroex_ethereum', 'weth9_evt_withdrawal') }}
    )

SELECT unique_transfer_id, 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, amount_raw
FROM sent_transfers
UNION
SELECT unique_transfer_id, 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, amount_raw
FROM received_transfers
UNION
SELECT unique_transfer_id, 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, amount_raw
FROM deposited_weth
UNION
SELECT unique_transfer_id, 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, amount_raw
FROM withdrawn_weth