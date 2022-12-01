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
    from {{ source('optimism', 'traces') }}
    where
      type = 'create'
      and success
      and tx_success
      {% if is_incremental() %}
      and block_time >= date_trunc('day', now() - interval '1 week')
      {% endif %}
)
SELECT
  cr.created_time
  ,cr.creation_tx_hash
  ,cr.contract_address
  ,cr.trace_element
from creates AS cr
join {{ source('optimism', 'traces') }} AS sd
  on cr.creation_tx_hash = sd.tx_hash
  and cr.created_time = sd.block_time
  and cr.trace_element = sd.trace_address[0]
  and sd.`type` = 'suicide'
  {% if is_incremental() %}
  and sd.block_time >= date_trunc('day', now() - interval '1 week')
  {% endif %}
group by 1, 2, 3, 4
;
