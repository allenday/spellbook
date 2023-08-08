{{ config(
        alias ='erc1155_agg_hour',
        materialized = 'view',
                incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
    TIMESTAMP_TRUNC(evt_block_time, hour) AS `hour`,
    wallet_address,
    token_address,
    tokenId,
    sum(amount) as amount,
    unique_tx_id || '-' || wallet_address || '-' || token_address || tokenId || '-' || sum(amount)::string as unique_transfer_id
FROM {{ ref('transfers_ethereum_erc1155') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where TIMESTAMP_TRUNC(evt_block_time, hour) > CURRENT_TIMESTAMP() - interval 2 days
{% endif %}
group by
    TIMESTAMP_TRUNC(evt_block_time, hour), wallet_address, token_address, tokenId, unique_tx_id