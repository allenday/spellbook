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
    a.reserve
    , t.symbol
    , date_trunc('hour', a.evt_block_time) AS hour
    , avg(cast(a.liquidityRate AS DOUBLE)) / 1e27 AS deposit_apy
    , avg(cast(a.stableBorrowRate AS DOUBLE)) / 1e27 AS stable_borrow_apy
    , avg(cast(a.variableBorrowRate AS DOUBLE)) / 1e27 AS variable_borrow_apy
FROM {{ source('aave_v2_ethereum', 'LendingPool_evt_ReserveDataUpdated') }} AS a
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} AS t
    ON a.reserve = t.contract_address
GROUP BY 1, 2, 3
