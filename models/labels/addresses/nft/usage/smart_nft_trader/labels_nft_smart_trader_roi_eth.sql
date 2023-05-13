{{ config(alias='nft_smart_trader_roi_eth') }}

with

aggregated_wallet_trading_stats as (
    select *
    from {{ ref('nft_ethereum_wallet_metrics') }}
    where
        trades_count >= 10
        and unique_collections_traded >= 3
        and spent_eth >= 1
        and spent_eth_realized >= 1
        and roi_eth > 0
),


aggregated_wallet_trading_stats_w_ranks as (
    select
        ROW_NUMBER() over (order by roi_eth_realized desc) as rank_roi,
        count(*) over () as total_count,
        *
    from aggregated_wallet_trading_stats
),


aggregated_wallet_trading_stats_w_label as (
    select
        'ethereum' as blockchain,
        wallet as address,
        case
            when
                (rank_roi * 1.00 / total_count) * 100 <= 3
                and (rank_roi * 1.00 / total_count) * 100 > 2
                then 'Top 3% Smart NFT Trader (ROI Realized in ETH w filters)'
            when
                (rank_roi * 1.00 / total_count) * 100 <= 2
                and (rank_roi * 1.00 / total_count) * 100 > 1
                then 'Top 2% Smart NFT Trader (ROI Realized in ETH w filters)'
            when (rank_roi * 1.00 / total_count) * 100 <= 1
                then 'Top 1% Smart NFT Trader (ROI Realized in ETH w filters)'
        end as name,
        'nft' as category,
        'NazihKalo' as contributor,
        'query' as source,
        date '2023-03-05' as created_at,
        current_timestamp as updated_at,
        'nft_traders_roi' as model_name,
        'usage' as label_type
        -- uncomment line below to see stats on the trader 
    -- , *  
    from aggregated_wallet_trading_stats_w_ranks order by roi_eth_realized desc
)

select *
from aggregated_wallet_trading_stats_w_label
where name is not null
