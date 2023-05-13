{{ config(
  schema = 'aave_v3_optimism'
  , materialized = 'incremental'
  , file_format = 'delta'
  , incremental_strategy = 'merge'
  , unique_key = ['reserve', 'symbol', 'hour']
  , alias='interest'
  , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "aave_v3",
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
from {{ source('aave_v3_optimism', 'Pool_evt_ReserveDataUpdated') }} as a
left join {{ ref('tokens_optimism_erc20') }} as t
    on CAST(a.reserve as VARCHAR(100)) = t.contract_address
{% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
{% endif %}
group by 1, 2, 3
