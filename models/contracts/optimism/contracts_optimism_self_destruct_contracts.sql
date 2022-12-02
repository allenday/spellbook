{{
  config(
        alias='self_destruct_contracts',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

WITH creates AS (
    SELECT
        block_time AS created_time
        , tx_hash AS creation_tx_hash
        , address AS contract_address
        , trace_address[0] AS trace_element
    FROM {{ source('optimism', 'traces') }}
    WHERE
        type = 'create'
        AND success
        AND tx_success
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - INTERVAL '1 week')
        {% endif %}
)

SELECT
    creates.created_time
    , creates.creation_tx_hash
    , creates.contract_address
    , creates.trace_element
FROM creates
INNER JOIN {{ source('optimism', 'traces') }} AS sd
           ON creates.creation_tx_hash = sd.tx_hash
           AND creates.created_time = sd.block_time
           AND creates.trace_element = sd.trace_address[0]
           AND sd.type = 'suicide'
           {% if is_incremental() %}
           AND sd.block_time >= date_trunc('day', now() - INTERVAL '1 week')
  {% endif %}
GROUP BY 1, 2, 3, 4
