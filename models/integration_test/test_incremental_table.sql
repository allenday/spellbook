{{ config(
        alias ='test_incremental_table',
        materialized = 'view',
                incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
    TIMESTAMP_TRUNC(evt_block_time, day) AS `day`,
    wallet_address,
    token_address,
    tokenId,
    sum(amount) as amount,
    unique_tx_id || '-' || wallet_address || '-' || token_address || tokenId || '-' || sum(amount)::string as unique_transfer_id
FROM {{ ref('test_view') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where TIMESTAMP_TRUNC(evt_block_time, day) > CURRENT_TIMESTAMP() - interval 2 days
{% endif %}
group by
    TIMESTAMP_TRUNC(evt_block_time, day), wallet_address, token_address, tokenId, unique_tx_id