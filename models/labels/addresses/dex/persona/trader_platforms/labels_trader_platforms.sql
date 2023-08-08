{{
    config(
        alias='trader_platforms'
    )
}}

with trader_platforms as (
    select taker           as address,
           MIN(block_time) as first_trade,
           COUNT(*)        as num_txs, -- to optimize query
           project,
           blockchain
    from (
        select blockchain,
               taker,
               project,
               block_time
        from {{ ref('dex_aggregator_trades') }}
        UNION ALL
        select blockchain,
               taker,
               project,
               block_time
        from {{ ref('dex_trades') }}
          )
    group by taker, project, blockchain
    order by first_trade
)

select blockchain,
       address,
       STRING_AGG(array_distinct(ARRAY_AGG(concat(upper(substring(project, 1, 1)), substring(project, 2)))),
                  ', ') || ' User' AS name,
       "dex"                       AS category,
       "gentrexha"                 AS contributor,
       "query"                     AS source,
       timestamp('2022-12-21')     AS created_at,
       CURRENT_TIMESTAMP()                       AS updated_at,
       "trader_platforms"          AS model_name,
       "persona"                   AS label_type
from trader_platforms
where address is not null
group by address, blockchain