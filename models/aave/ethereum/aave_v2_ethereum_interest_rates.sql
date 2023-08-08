{{ config(
  schema = 'aave_v2_ethereum'
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
from {{ source('aave_v2_ethereum', 'LendingPool_evt_ReserveDataUpdated') }} a
left join {{ ref('tokens_ethereum_erc20') }} t
on CAST(a.reserve AS STRING) = t.contract_address
group by 1,2,3