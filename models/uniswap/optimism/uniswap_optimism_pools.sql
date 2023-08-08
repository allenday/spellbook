 {{
  config(
        schema='uniswap_v3_optimism',
        alias='pools',
        materialized = 'view'
  )
}}
with uniswap_v3_poolcreated as (
  select 
    pool
    ,token0
    ,token1
    ,fee
  from {{ source('uniswap_v3_optimism', 'factory_evt_poolcreated') }} 
  group by 1, 2, 3, 4
)

select 
   newAddress as pool
  , LOWER(token0) AS token0
  , LOWER(token1) AS token1
  ,fee
from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}

UNION ALL

select
  pool
  , LOWER(token0) AS token0
  , LOWER(token1) AS token1
  , fee
from uniswap_v3_poolcreated