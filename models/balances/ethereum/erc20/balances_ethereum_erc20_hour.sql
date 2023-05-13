{{ config(
        alias='erc20_hour',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke","dot2dotseurat"]\') }}'
        )
}}

with
hours as (
    select
        explode(
            sequence(
                to_date('2015-01-01'), date_trunc('hour', now()), interval 1 hour
            )
        ) as hour
),

hourly_balances as (
    select
        wallet_address,
        token_address,
        amount_raw,
        amount,
        hour,
        symbol,
        lead(hour, 1, now()) over (partition by token_address, wallet_address order by hour) as next_hour
    from {{ ref('transfers_ethereum_erc20_rolling_hour') }}
)

select
    'ethereum' as blockchain,
    h.hour,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
from hourly_balances as b
inner join hours as h on b.hour <= h.hour and h.hour < b.next_hour
left join {{ source('prices', 'usd') }} as p
    on
        p.contract_address = b.token_address
        and h.hour = p.minute
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
