{{ config(
        alias='erc1155_hour',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke"]\') }}'
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
        b.wallet_address,
        b.token_address,
        b.tokenid,
        b.hour,
        b.amount,
        lead(b.hour, 1, now()) over (partition by b.wallet_address, b.token_address, b.tokenid order by hour) as next_hour
    from {{ ref('transfers_ethereum_erc1155_rolling_hour') }} as b
)

select
    'ethereum' as blockchain,
    d.hour,
    b.wallet_address,
    b.token_address,
    b.tokenid,
    SUM(b.amount) as amount,
    nft_tokens.name as collection
from hourly_balances as b
inner join hours as d on b.hour <= d.hour and d.hour < b.next_hour
left join {{ ref('tokens_nft') }} as nft_tokens
    on
        nft_tokens.contract_address = b.token_address
        and nft_tokens.blockchain = 'ethereum'
group by 1, 2, 3, 4, 5, 7
having SUM(b.amount) > 0
