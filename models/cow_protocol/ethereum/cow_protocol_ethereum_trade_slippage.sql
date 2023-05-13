{{ config(alias='trade_slippage',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha", "josojo"]\') }}'
) }}

-- PoC Query: https://dune.com/queries/2279196
with
raw_data as (
    select
        order_uid,
        block_number,
        block_time,
        order_type,
        case
            when order_type = 'SELL'
                then cast(atoms_sold as double) / cast(limit_sell_amount as double)
            else cast(atoms_bought as double) / cast(limit_buy_amount as double)
        end as fill_proportion,
        case
            when order_type = 'SELL'
                then (cast(limit_buy_amount as double) / (1.0 - (cast(slippage_bips as double) / 10000.0)))
            else cast(limit_buy_amount as double)
        end as buy_quote,
        atoms_bought,
        case
            when order_type = 'BUY'
                then (cast(limit_sell_amount as double) / (1.0 + (cast(slippage_bips as double) / 10000.0)))
            else cast(limit_sell_amount as double)
        end as sell_quote,
        atoms_sold,
        usd_value as trade_usd_value,
        slippage_bips as tolerance_bips
    from {{ ref('cow_protocol_ethereum_app_data') }} as ad
    inner join {{ ref('cow_protocol_ethereum_trades') }} as t on t.app_data = ad.app_hash
    where slippage_bips is not null
),

results as (
    select
        order_uid,
        block_number,
        block_time,
        buy_quote,
        sell_quote,
        tolerance_bips,
        trade_usd_value,
        fill_proportion,
        case
            when order_type = 'SELL' then (atoms_bought - (buy_quote * fill_proportion))
            else ((sell_quote * fill_proportion) - atoms_sold)
        end as amount_atoms,
        100.0 * (case
            when order_type = 'SELL'
                then (atoms_bought - (buy_quote * fill_proportion)) / (buy_quote * fill_proportion)
            else ((sell_quote * fill_proportion) - atoms_sold) / (sell_quote * fill_proportion)
        end) as amount_percentage,
        case
            when order_type = 'SELL'
                then (atoms_bought - (buy_quote * fill_proportion)) * (trade_usd_value / atoms_bought)
            else ((sell_quote * fill_proportion) - atoms_sold) * (trade_usd_value / atoms_sold)
        end as amount_usd
    from raw_data
)

select * from results
