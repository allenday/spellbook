{{
    config(
        alias='dex_traders',
        post_hook='{{ expose_spells(\'["ethereum", "fantom", "arbitrum", "gnosis", "optimism", "polygon"]\', 
        "sector", 
        "labels", 
        \'["gentrexha", "Henrystats"]\') }}'
    )
}}

with
dex_traders as (
    select
        address,
        blockchain
    from (
        select
            taker as address,
            blockchain
        from {{ ref('dex_trades') }}
        group by taker, blockchain  --distinct
        union all
        select
            tx_from as address,
            blockchain
        from {{ ref('dex_trades') }}
        group by tx_from, blockchain --distinct
    ) as uni
    group by address, blockchain--distinct
)

select
    blockchain,
    address,
    "DEX Trader" as name,
    "dex" as category,
    "gentrexha" as contributor,
    "query" as source,
    timestamp("2022-12-14") as created_at,
    now() as updated_at,
    "dex_traders" as model_name,
    "persona" as label_type
from
    dex_traders
