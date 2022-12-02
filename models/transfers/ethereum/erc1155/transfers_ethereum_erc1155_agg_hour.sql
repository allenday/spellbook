{{ config(
        alias ='erc1155_agg_hour',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

SELECT
    'ethereum' AS blockchain
    , wallet_address
    , token_address
    , tokenId
    , date_trunc('hour', evt_block_time) AS hour
    , sum(amount) AS amount
    , unique_tx_id || '-' || wallet_address || '-' || token_address || tokenId || '-' || sum(
        amount
    )::STRING AS unique_transfer_id
FROM {{ ref('transfers_ethereum_erc1155') }}
{% if is_incremental() %}
    -- this filter will only be applied ON an incremental run
    WHERE date_trunc('hour', evt_block_time) > now() - INTERVAL 2 DAYS
{% endif %}
GROUP BY
    date_trunc('hour', evt_block_time)
    , wallet_address
    , token_address
    , tokenId
    , unique_tx_id
