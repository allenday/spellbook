{{
    config(
        alias='dex_aggregator_traders'
    )
}}

with
 dex_traders as (
    select distinct taker as address, blockchain
    from {{ref('dex_aggregator_trades')}}
  )
select
  blockchain,
  address,
  "DEX Aggregator Trader" AS name,
  "dex" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-14') as created_at,
  CURRENT_TIMESTAMP() as updated_at,
  "dex_aggregator_traders" as model_name,
  "persona" as label_type
from
  dex_traders