{{ config(
    schema = 'addresses_events_optimism'
    , alias = 'first_funded_by'
    , materialized = 'view'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

SELECT 'optimism' AS blockchain
, et.to AS address
, ARRAY_AGG(et.from ORDER BY et.block_number ASC LIMIT 1)[OFFSET(0)] AS first_funded_by
, MIN(et.block_time) AS block_time
, MIN(et.block_number) AS block_number
, ARRAY_AGG(et.tx_hash ORDER BY et.block_number ASC LIMIT 1)[OFFSET(0)] AS tx_hash
FROM {{ source('optimism', 'traces') }} et
{% if is_incremental() %}
LEFT ANTI JOIN {{this}} ffb ON et.to = ffb.address
{% endif %}
WHERE et.success
AND (et.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR et.call_type IS NULL)
AND CAST(et.value AS FLOAT64) > 0
{% if is_incremental() %}
AND et.block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}
GROUP BY et.to