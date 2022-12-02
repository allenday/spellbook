{{ config(
        alias ='test_incremental_table',
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
    sum(amount) AS amount,
    unique_tx_id || '-' || wallet_address || '-' || token_address || tokenId || '-' || sum(amount)::STRING AS unique_transfer_id
FROM {{ ref('test_view') }}
{% if is_incremental() %}
    -- this filter will only be applied ON an incremental run
    where date_trunc('day', evt_block_time) > now() - INTERVAL 2 days
{% endif %}
GROUP BY
    date_trunc('day', evt_block_time), wallet_address, token_address, tokenId, unique_tx_id