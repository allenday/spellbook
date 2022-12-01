{{ config(materialized='view', alias='test_view') }}

with
  erc1155_ids_batch AS (
    SELECT
      *,
      explode(ids) AS explode_id,
      evt_tx_hash || '-' || cast(
        ROW_NUMBER() OVER (
          PARTITION BY evt_tx_hash,
          ids
          ORDER BY
            ids
        ) AS STRING
      ) AS unique_transfer_id
    FROM {{source('erc1155_ethereum', 'evt_transferbatch')}}
      limit 100
  ),

  erc1155_values_batch AS (
    SELECT
      *,
      explode(
        values
      ) AS explode_value,
      evt_tx_hash || '-' || cast(
        ROW_NUMBER() OVER (
          PARTITION BY evt_tx_hash,
          ids
          ORDER BY
            ids
        ) AS STRING
      ) AS unique_transfer_id
    FROM {{source('erc1155_ethereum', 'evt_transferbatch')}}
            limit 100
  ),

  erc1155_transfers_batch AS (
    SELECT
      DISTINCT erc1155_ids_batch.explode_id,
      erc1155_values_batch.explode_value,
      erc1155_ids_batch.evt_tx_hash,
      erc1155_ids_batch.to,
      erc1155_ids_batch.from,
      erc1155_ids_batch.contract_address,
      erc1155_ids_batch.evt_index,
      erc1155_ids_batch.evt_block_time
    FROM erc1155_ids_batch
      JOIN erc1155_values_batch ON erc1155_ids_batch.unique_transfer_id = erc1155_values_batch.unique_transfer_id
    limit 100
  ),

  sent_transfers AS (
    SELECT
      evt_tx_hash,
      evt_tx_hash || '-' || evt_index || '-' || to AS unique_tx_id,
      to AS wallet_address,
      contract_address AS token_address,
      evt_block_time,
      id AS tokenId,
      value AS amount
    FROM {{source('erc1155_ethereum', 'evt_transfersingle')}} single
    UNION ALL
    SELECT
      evt_tx_hash,
      evt_tx_hash || '-' || evt_index || '-' || to AS unique_tx_id,
      to AS wallet_address,
      contract_address AS token_address,
      evt_block_time,
      explode_id AS tokenId,
      explode_value AS amount
    FROM erc1155_transfers_batch
          limit 100
  ),

  received_transfers AS (
    SELECT
      evt_tx_hash,
      evt_tx_hash || '-' || evt_index || '-' || to AS unique_tx_id,
      FROM AS wallet_address,
      contract_address AS token_address,
      evt_block_time,
      id AS tokenId,
      - value AS amount
    FROM {{source('erc1155_ethereum', 'evt_transfersingle')}}
    UNION ALL
    SELECT
      evt_tx_hash,
      evt_tx_hash || '-' || evt_index || '-' || to AS unique_tx_id,
      FROM AS wallet_address,
      contract_address AS token_address,
      evt_block_time,
      explode_id AS tokenId,
      - explode_value AS amount
    FROM erc1155_transfers_batch
    limit 100
  )

SELECT 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, evt_tx_hash, unique_tx_id
FROM sent_transfers
UNION ALL
SELECT 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, evt_tx_hash, unique_tx_id
FROM received_transfers
limit 100
