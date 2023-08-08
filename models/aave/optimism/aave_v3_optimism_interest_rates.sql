{{ config(
  schema = 'aave_v3_optimism'
  , materialized = 'view'
  , file_format = 'delta'
  , incremental_strategy = 'merge'
  , unique_key = ['reserve', 'symbol', 'hour']
  , alias='interest'
  
  )
}}

select 
  a.reserve, 
  t.symbol,
  TIMESTAMP_TRUNC(a.evt_block_time, hour) AS `hour`, 
  avg(CAST(a.liquidityRate AS FLOAT64)) / 1e27 as deposit_apy, 
  avg(CAST(a.stableBorrowRate AS FLOAT64)) / 1e27 as stable_borrow_apy, 
  avg(CAST(a.variableBorrowRate AS FLOAT64)) / 1e27 as variable_borrow_apy
from {{ source('aave_v3_optimism', 'Pool_evt_ReserveDataUpdated') }} a
left join {{ ref('tokens_optimism_erc20') }} t
on CAST(a.reserve AS STRING) = t.contract_address
{% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}
group by 1,2,3