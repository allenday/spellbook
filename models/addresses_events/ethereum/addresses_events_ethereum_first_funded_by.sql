{{ config
(
    alias='first_funded_by'
    , materialized = 'incremental'
    , file_format = 'delta'
    , post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses_events",
                                    \'["hildobby"]\') }}'
    , unique_key = ['address']
)
}}

SELECT b.to AS address
    , MIN(a.from) AS first_funded_by
    , MIN(a.block_time) AS block_time
    , MIN(a.block_number) AS block_number
    , MIN(a.tx_hash) AS tx_hash
FROM {{ source('ethereum', 'traces') }} a
JOIN
(
    SELECT et.to
        , MIN(et.block_number) AS first_block
    FROM {{ source('ethereum', 'traces') }} et
    {% if is_incremental() %}
    LEFT JOIN {{this}} ffb
        ON et.to = ffb.address
    {% endif %}
    WHERE et.success IS TRUE
    AND (et.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR et.call_type IS NULL)
    AND et.value > 0
    {% if is_incremental() %}
    AND ffb.address IS NULL
    {% if date_trunc_variant == 1 %}
    AND et.block_time >= date_trunc('day', CURRENT_TIMESTAMP - interval '1 week')
    {% elif date_trunc_variant == 2 %}
    AND et.block_time >= date_trunc(CURRENT_TIMESTAMP - interval 1 week, day)
    {% endif %}
    {% endif %}
    GROUP BY et.to
) AS b 
ON a.to = b.to
    AND a.block_number = b.first_block
WHERE a.success IS TRUE
    AND (a.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR a.call_type IS NULL)
    AND a.value > 0
    {% if is_incremental() %}
    {% if date_trunc_variant == 1 %}
    AND a.block_time >= date_trunc('day', CURRENT_TIMESTAMP - interval '1 week')
    {% elif date_trunc_variant == 2 %}
    AND a.block_time >= date_trunc(CURRENT_TIMESTAMP - interval 1 week, day)
    {% endif %}
    {% endif %}
GROUP BY b.to
