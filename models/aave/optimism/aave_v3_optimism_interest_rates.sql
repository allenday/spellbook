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
  DATE_TRUNC('hour',a.evt_block_time) AS hour,
  AVG(CAST(a.liquidityRate AS DOUBLE)) / 1e27 AS deposit_apy,
  AVG(CAST(a.stableBorrowRate AS DOUBLE)) / 1e27 AS stable_borrow_apy,
  AVG(CAST(a.variableBorrowRate AS DOUBLE)) / 1e27 AS variable_borrow_apy
FROM {{ source('aave_v3_optimism', 'Pool_evt_ReserveDataUpdated') }} AS a
LEFT JOIN {{ ref('tokens_optimism_erc20') }} AS t
ON a.reserve = t.contract_address
GROUP BY 1,2,3
