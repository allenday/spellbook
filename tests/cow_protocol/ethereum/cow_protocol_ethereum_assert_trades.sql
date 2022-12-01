-- Try it out here: https://dune.com/queries/1398185
SELECT evt_tx_hash
FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Trade') }}
WHERE
    evt_tx_hash NOT IN (
        SELECT tx_hash
        FROM {{ ref('cow_protocol_ethereum_batches') }}
    )
    AND evt_block_time < date(now()) - INTERVAL '1 day'
