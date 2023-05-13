{{ config(
  schema = 'aave_v2_ethereum'
  , alias='interest'
  , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "aave_v2",
                                  \'["batwayne", "chuxin"]\') }}'
  )
}}

select
    a.reserve,
    t.symbol,
    date_trunc('hour', a.evt_block_time) as hour,
    avg(CAST(a.liquidityrate as DOUBLE)) / 1e27 as deposit_apy,
    avg(CAST(a.stableborrowrate as DOUBLE)) / 1e27 as stable_borrow_apy,
    avg(CAST(a.variableborrowrate as DOUBLE)) / 1e27 as variable_borrow_apy
from {{ source('aave_v2_ethereum', 'LendingPool_evt_ReserveDataUpdated') }} as a
left join {{ ref('tokens_ethereum_erc20') }} as t
    on CAST(a.reserve as VARCHAR(100)) = t.contract_address
group by 1, 2, 3;
