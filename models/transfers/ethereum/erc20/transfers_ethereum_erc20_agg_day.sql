{{ config(
        alias ='erc20_agg_day',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

SELECT
    'ethereum' AS blockchain,
    date_trunc('day', tr.evt_block_time) AS day,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    tr.wallet_address || '-' || tr.token_address || '-' || date_trunc('day', tr.evt_block_time) AS unique_transfer_id,
    sum(tr.amount_raw) AS amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) AS amount
from {{ ref('transfers_ethereum_erc20') }} tr
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.evt_block_time >= date_trunc('day', now() - interval '1 week')
{% endif %}
group by 1, 2, 3, 4, 5, 6
