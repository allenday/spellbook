{{ config(
  schema = 'aave_v3_optimism'
  , alias='interest'
  , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "aave_v3",
                                  \'["batwayne", "chuxin"]\') }}'
  )
}}

SELECT
  a.reserve,
  t.symbol,
  date_trunc('hour',a.evt_block_time) AS hour,
  avg(CAST(a.liquidityRate AS DOUBLE)) / 1e27 AS deposit_apy,
  avg(CAST(a.stableBorrowRate AS DOUBLE)) / 1e27 AS stable_borrow_apy,
  avg(CAST(a.variableBorrowRate AS DOUBLE)) / 1e27 AS variable_borrow_apy
from {{ source('aave_v3_optimism', 'Pool_evt_ReserveDataUpdated') }} AS a
LEFT JOIN {{ ref('tokens_optimism_erc20') }} AS t
on a.reserve=t.contract_address
group by 1,2,3
