{{ config(
        alias ='transfers',
        partition_by='block_date',
        materialized = 'view',
        unique_key = ['unique_transfer_id']
)
}}

WITH reservoir AS (
    SELECT *
    FROM {{ source('erc721_ethereum','evt_transfer') }}
    UNION ALL
    SELECT *
    FROM {{ source('erc1155_ethereum','evt_transfersingle') }}
    UNION ALL
    SELECT *
    FROM {{ source('erc1155_ethereum', 'evt_transferbatch') }}
), reservoir_fixed AS (
    SELECT r_a.*
    FROM reservoir r_a
    LEFT JOIN {{this}} r_b
        ON r_a.evt_tx_hash = r_b.tx_hash
    WHERE r_b.tx_hash IS NULL
)

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, TIMESTAMP_TRUNC(t.evt_block_time, DAY) AS block_date
, t.evt_block_number AS block_number
, 'erc721' AS token_standard
, 'single' AS transfer_type
, t.evt_index
, t.contract_address
, t.tokenId AS token_id
, 1 AS amount
, t.from
, t.to
, et.from AS executed_by
, t.evt_tx_hash AS tx_hash
, CONCAT('ethereum', t.evt_tx_hash, '-erc721-', t.contract_address, '-', t.tokenId, '-', t.from, '-', t.to, '-1-', t.evt_index) AS unique_transfer_id
FROM reservoir_fixed t
INNER JOIN {{ source('ethereum', 'transactions') }} et 
    ON et.block_number = t.evt_block_number
    AND et.hash = t.evt_tx_hash
{% if is_incremental() %}
    AND TIMESTAMP_TRUNC(et.block_time, DAY) >= TIMESTAMP_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
WHERE TIMESTAMP_TRUNC(t.evt_block_time, DAY) >= TIMESTAMP_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
{% endif %}

/* Similarly, modify other SELECT queries as well, replacing ANTI JOIN with LEFT JOIN...IS NULL and adjusting ARRAY functions */
