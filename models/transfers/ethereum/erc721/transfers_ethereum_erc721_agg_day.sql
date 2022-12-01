{{ config(
        alias ='erc721_agg_day',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

SELECT
    'ethereum' AS blockchain,
    date_trunc('day', evt_block_time) AS day,
    wallet_address,
    token_address,
    tokenId,
    wallet_address || '-' || date_trunc('day', evt_block_time) || '-' || token_address || '-' || tokenId AS unique_transfer_id
from {{ ref('transfers_ethereum_erc721') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where evt_block_time >= date_trunc('hour', now() - interval '1 week')
{% endif %}
group by 1,2,3,4,5
having sum(amount) = 1
