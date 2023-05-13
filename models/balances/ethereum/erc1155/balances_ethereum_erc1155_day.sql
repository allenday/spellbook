{{ config(
        alias='erc1155_day',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke"]\') }}'
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
    from {{ ref('transfers_ethereum_erc1155_rolling_day') }} as b
)

select
    'ethereum' as blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.tokenid,
    SUM(b.amount) as amount,
    nft_tokens.name as collection
from daily_balances as b
inner join days as d on b.day <= d.day and d.day < b.next_day
left join {{ ref('tokens_nft') }} as nft_tokens
    on
        nft_tokens.contract_address = b.token_address
        and nft_tokens.blockchain = 'ethereum'
group by 1, 2, 3, 4, 5, 7
having SUM(b.amount) > 0
