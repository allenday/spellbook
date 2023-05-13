{{ config(
        alias ='erc721_rolling_hour')
}}

select
    'ethereum' as blockchain,
    hour,
    wallet_address,
    token_address,
    tokenid,
    NOW() as updated_at,
    sum(amount) over (partition by wallet_address, token_address, tokenid order by hour) as amount,
    row_number() over (partition by wallet_address, token_address, tokenid order by hour desc) as recency_index
from {{ ref('transfers_ethereum_erc721_agg_hour') }};
