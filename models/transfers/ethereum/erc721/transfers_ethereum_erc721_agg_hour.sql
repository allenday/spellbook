{{ config(
        alias ='erc721_agg_hour',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

SELECT
    'ethereum' AS blockchain
    , date_trunc('hour', evt_block_time) AS hour
    , wallet_address
    , token_address
    , tokenId
    , wallet_address || '-' || date_trunc(
        'hour', evt_block_time
    ) || '-' || token_address || '-' || tokenId AS unique_transfer_id
FROM {{ ref('transfers_ethereum_erc721') }}
{% if is_incremental() %}
    -- this filter will only be applied ON an incremental run
    WHERE evt_block_time >= date_trunc('day', now() - INTERVAL '1 week')
{% endif %}
GROUP BY 1, 2, 3, 4, 5
HAVING sum(amount) = 1
