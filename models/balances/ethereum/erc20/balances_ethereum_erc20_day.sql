{{ config(
        alias='erc20_day',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke","dot2dotseurat"]\') }}'
        )
}}

with
days as (
    select
        explode(
            sequence(
                to_date('2015-01-01'), date_trunc('day', now()), interval 1 day
            )
        ) as day
),

daily_balances as (
    select
        wallet_address,
        token_address,
        amount_raw,
        amount,
        day,
        symbol,
        lead(day, 1, now()) over (partition by token_address, wallet_address order by day) as next_day
    from {{ ref('transfers_ethereum_erc20_rolling_day') }}
)

select
    'ethereum' as blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
from daily_balances as b
inner join days as d on b.day <= d.day and d.day < b.next_day
left join {{ source('prices', 'usd') }} as p
    on
        p.contract_address = b.token_address
        and d.day = p.minute
        and p.blockchain = 'ethereum'
-- Removes rebase tokens from balances
left join {{ ref('tokens_ethereum_rebase') }} as r
    on b.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
left join {{ ref('balances_ethereum_erc20_noncompliant') }} as nc
    on b.token_address = nc.token_address
where
    r.contract_address is null
    and nc.token_address is null
