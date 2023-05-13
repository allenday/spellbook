{{ config(alias='aave_v2_deposit_size') }}

with latest_net_deposits as (
    with annual_flows as (
        select
            d.evt_block_time,
            d.user,
            cast(d.amount as double) / pow(10, p.decimals) as amount,
            d.reserve,
            p.symbol,
            d.contract_address as lendingpool_contract
        from {{ source('aave_v2_ethereum', 'LendingPool_evt_Deposit') }} as d
        left join {{ ref('prices_tokens') }} as p
            on
                p.contract_address = d.reserve
                and p.blockchain = 'ethereum'
        where evt_block_time > NOW() - interval '1' day

        union all

        select
            w.evt_block_time,
            w.user,
            cast(w.amount as double) * -1 / pow(10, p.decimals) as amount,
            w.reserve,
            p.symbol,
            w.contract_address as lendingpool_contract
        from {{ source('aave_v2_ethereum', 'LendingPool_evt_Withdraw') }} as w
        left join {{ ref('prices_tokens') }} as p
            on
                p.contract_address = w.reserve
                and p.blockchain = 'ethereum'
        where evt_block_time > NOW() - interval '1' day
    ),

    annual_net_deposit as (
        select
            user,
            SUM(amount) as net_deposit,
            reserve,
            symbol,
            evt_block_time
        from annual_flows
        where amount > 0
        group by 1, 3, 4, 5
        order by net_deposit desc
    ),

    ordered_annual_net_deposit as (
        select
            row_number() over (partition by a.user, a.reserve order by a.evt_block_time desc) as rn,
            a.user,
            a.net_deposit,
            a.reserve,
            a.symbol,
            a.evt_block_time
        from annual_net_deposit as a
    )

    select
        user,
        net_deposit,
        reserve,
        symbol,
        evt_block_time
    from ordered_annual_net_deposit
    where rn = 1
    order by net_deposit desc
),

latest_available_liquidity as (
    with cte as (
        select
            row_number() over (partition by d.reserve, p.symbol order by d.call_block_time desc) as rn,
            d.reserve,
            p.symbol,
            cast(d.availableliquidity as double) / pow(10, p.decimals) as available_liquidity,
            d.call_block_time
        from {{ source('aave_v2_ethereum', 'DefaultReserveInterestRateStrategy_call_calculateInterestRates') }} as d
        left join {{ ref('prices_tokens') }} as p
            on
                p.contract_address = d.reserve
                and p.blockchain = 'ethereum'
        order by call_block_time desc
    )

    select
        reserve,
        symbol,
        available_liquidity,
        call_block_time
    from cte
    where rn = 1
    order by call_block_time asc
),

calc_deposit_pct as (
    select
        d.user,
        d.net_deposit,
        l.available_liquidity,
        d.net_deposit / l.available_liquidity * 100 as largest_deposit_pct,
        d.reserve,
        d.symbol,
        d.evt_block_time
    from latest_net_deposits as d
    left join latest_available_liquidity as l on d.reserve = l.reserve and d.symbol = l.symbol
),

final_base_label as (
    select
        'ethereum' as blockchain,
        user as address,
        case
            when COALESCE(largest_deposit_pct, 0) <= 1 then 'Small Depositor'
            when largest_deposit_pct <= 10 then 'Sizeable Depositor'
            else 'Large Depositor'
        end as name,
        'deposit size' as category,
        'paulx' as contributor,
        'wizard' as source,
        date('2023-03-26') as created_at,
        now() as updated_at,
        'aave_v2 daily deposits' as model_name,
        'persona' as label_type,
        net_deposit,
        available_liquidity,
        largest_deposit_pct,
        symbol
    from calc_deposit_pct
)

select
    blockchain,
    address,
    name,
    category,
    contributor,
    source,
    created_at,
    updated_at,
    model_name,
    label_type
from final_base_label;
