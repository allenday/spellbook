{{ config(
        alias ='transfers',
        partition_by='block_date',
        materialized = 'view',
                unique_key = ['unique_transfer_id']
)
}}

 SELECT 'ethereum' as blockchain
, t.evt_block_time AS block_time
, TIMESTAMP_TRUNC(t.evt_block_time, day) AS block_date
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
, 'ethereum' || t.evt_tx_hash || '-erc721-' || t.contract_address || '-' || t.tokenId || '-' || t.from || '-' || t.to || '-' || '1' || '-' || t.evt_index AS unique_transfer_id
FROM {{ source('erc721_ethereum','evt_transfer') }} t
    {% if is_incremental() %}
        ANTI JOIN {{this}} anti_table
            ON t.evt_tx_hash = anti_table.tx_hash
    {% endif %}
INNER JOIN {{ source('ethereum', 'transactions') }} et ON et.block_number = t.evt_block_number
    AND et.hash = t.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'ethereum' as blockchain
, t.evt_block_time AS block_time
, TIMESTAMP_TRUNC(t.evt_block_time, day) AS block_date
, t.evt_block_number AS block_number
, 'erc1155' AS token_standard
, 'single' AS transfer_type
, t.evt_index
, t.contract_address
, t.id AS token_id
, t.value AS amount
, t.from
, t.to
, et.from AS executed_by
, t.evt_tx_hash AS tx_hash
, 'ethereum' || t.evt_tx_hash || '-erc1155-' || t.contract_address || '-' || t.id || '-' || t.from || '-' || t.to || '-' || t.value || '-' || t.evt_index AS unique_transfer_id
FROM {{ source('erc1155_ethereum','evt_transfersingle') }} t
    {% if is_incremental() %}
        ANTI JOIN {{this}} anti_table
            ON t.evt_tx_hash = anti_table.tx_hash
    {% endif %}
INNER JOIN {{ source('ethereum', 'transactions') }} et ON et.block_number = t.evt_block_number
    AND et.hash = t.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}

UNION ALL

    SELECT 
        'ethereum' as blockchain,
        t.evt_block_time AS block_time,
        TIMESTAMP_TRUNC(t.evt_block_time, DAY) AS block_date,
        t.evt_block_number AS block_number,
        'erc1155' AS token_standard,
        'batch' AS transfer_type,
        t.evt_index,
        t.contract_address,
        t.id AS token_id,
        t.value AS amount,
        t.from,
        t.to,
        et.from AS executed_by,
        t.evt_tx_hash AS tx_hash,
        CONCAT('ethereum', t.evt_tx_hash, '-erc1155-', t.contract_address, '-', CAST(id AS STRING), '-', t.from, '-', t.to, '-', CAST(t.value AS STRING), '-', CAST(t.evt_index AS STRING)) AS unique_transfer_id
    FROM (
        SELECT 
            t.evt_block_time, 
            t.evt_block_number, 
            t.evt_tx_hash, 
            t.contract_address, 
            t.from, 
            t.to, 
            t.evt_index,
            id,
            value
        FROM 
            {{ source('erc1155_ethereum', 'evt_transferbatch') }} t,
            UNNEST(t.ids) AS id WITH OFFSET id_offset,
            UNNEST(t.values) AS `value` WITH OFFSET value_offset
        WHERE 
            id_offset = value_offset
        {% if is_incremental() %}
            ANTI JOIN {{this}} anti_table
                ON t.evt_tx_hash = anti_table.tx_hash
        {% endif %}
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
        GROUP BY t.evt_block_time, t.evt_block_number, t.evt_tx_hash, t.contract_address, t.from, t.to, evt_index, id, value

    ) t
    INNER JOIN {{ source('ethereum', 'transactions') }} et 
    ON et.block_number = t.evt_block_number
    WHERE t.value > 0
    AND et.hash = t.evt_tx_hash