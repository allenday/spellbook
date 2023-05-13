{{ config(
        alias='erc721_day',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat"]\') }}'
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
        b.wallet_address,
        b.token_address,
        b.tokenid,
        b.day,
        b.amount,
        lead(b.day, 1, now()) over (partition by b.wallet_address, b.token_address, b.tokenid order by day) as next_day
    from {{ ref('transfers_ethereum_erc721_rolling_day') }} as b
    left join {{ ref('balances_ethereum_erc721_noncompliant') }} as nc
        on b.token_address = nc.token_address
    where nc.token_address is null
)

select
    'ethereum' as blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.tokenid,
    nft_tokens.name as collection
from daily_balances as b
inner join days as d on b.day <= d.day and d.day < b.next_day
left join {{ ref('tokens_nft') }} as nft_tokens
    on
        nft_tokens.contract_address = b.token_address
        and nft_tokens.blockchain = 'ethereum'
where b.amount = 1;
