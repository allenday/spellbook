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

with creates AS (
    SELECT
      block_time AS created_time
      ,tx_hash AS creation_tx_hash
      ,address AS contract_address
      ,trace_address[0] AS trace_element
    FROM {{ source('optimism', 'traces') }}
    where
      type = 'create'
      AND success
      AND tx_success
      {% if is_incremental() %}
      AND block_time >= date_trunc('day', now() - interval '1 week')
      {% endif %}
)
SELECT
  cr.created_time
  ,cr.creation_tx_hash
  ,cr.contract_address
  ,cr.trace_element
FROM creates AS cr
join {{ source('optimism', 'traces') }} AS sd
  ON cr.creation_tx_hash = sd.tx_hash
  AND cr.created_time = sd.block_time
  AND cr.trace_element = sd.trace_address[0]
  AND sd.`type` = 'suicide'
  {% if is_incremental() %}
  AND sd.block_time >= date_trunc('day', now() - interval '1 week')
  {% endif %}
GROUP BY 1, 2, 3, 4
;
