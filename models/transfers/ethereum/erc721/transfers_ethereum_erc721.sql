{{ config(materialized='view', alias='erc721') }}

with
    received_transfers AS (
        SELECT 'receive' || '-' ||  evt_tx_hash || '-' || evt_index || '-' || `to` AS unique_tx_id,
            to AS wallet_address,
            contract_address AS token_address,
            evt_block_time,
            tokenId,
            1 AS amount
        FROM
            {{ source('erc721_ethereum', 'evt_transfer') }}
    )

    ,
    sent_transfers AS (
        SELECT 'send' || '-' || evt_tx_hash || '-' || evt_index || '-' || `FROM` AS unique_tx_id,
            FROM AS wallet_address,
            contract_address AS token_address,
            evt_block_time,
            tokenId,
            -1 AS amount
        FROM
            {{ source('erc721_ethereum', 'evt_transfer') }}
    )

SELECT 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
FROM received_transfers
UNION
SELECT 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
FROM sent_transfers