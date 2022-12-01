{{ config(
  schema = 'aave_v2_ethereum'
  , alias='interest'
  , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave_v2",
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
from {{ source('aave_v2_ethereum', 'LendingPool_evt_ReserveDataUpdated') }} a
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} t
on a.reserve=t.contract_address
group by 1,2,3
;