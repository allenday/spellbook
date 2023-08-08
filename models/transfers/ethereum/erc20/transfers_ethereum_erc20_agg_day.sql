{{ config(
        alias ='erc20_agg_day',
        materialized = 'view',
                incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
    TIMESTAMP_TRUNC(tr.evt_block_time, day) AS `day`,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    tr.wallet_address || '-' || tr.token_address || '-' || TIMESTAMP_TRUNC(tr.evt_block_time, day) as unique_transfer_id,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
from {{ ref('transfers_ethereum_erc20') }} tr
left join {{ ref('tokens_ethereum_erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.evt_block_time >= date_trunc('day', CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}
group by 1, 2, 3, 4, 5, 6