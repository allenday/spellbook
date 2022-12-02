{{ config(
        alias ='erc20_agg_hour',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

SELECT
    'ethereum' AS blockchain,
    date_trunc('hour', tr.evt_block_time) AS hour,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    tr.wallet_address || '-' || tr.token_address || '-' || date_trunc('hour', tr.evt_block_time) AS unique_transfer_id,
    sum(tr.amount_raw) AS amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) AS amount
FROM {{ ref('transfers_ethereum_erc20') }} AS tr
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} AS t ON t.contract_address = tr.token_address
{% if is_incremental() %}
    -- this filter will only be applied ON an incremental run
    where tr.evt_block_time >= date_trunc('hour', now() - INTERVAL '1 week')
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6
