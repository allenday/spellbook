{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

SELECT
    'solana' AS blockchain,
    'opensea' AS project,
    'v1' AS version,
    signatures[0] AS tx_hash,
    block_date,
    block_time,
    CAST(block_slot AS BIGINT) AS block_number,
    abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
    abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) AS amount_original,
    CAST(abs(post_balances[0] - pre_balances[0]) AS DECIMAL(38, 0)) AS amount_raw,
    p.symbol AS currency_symbol,
    p.contract_address AS currency_contract,
    'metaplex' AS token_standard,
    CASE
        WHEN (array_contains(account_keys, '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y')) THEN '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y'
        WHEN (array_contains(account_keys, 'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r')) THEN 'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r'
    END AS project_contract_address,
    'Trade' AS evt_type,
    signatures[0] || '-' || id AS unique_trade_id
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ source('prices', 'usd') }} AS p
    ON
        p.minute = date_trunc('minute', block_time)
        AND p.blockchain IS NULL
        AND p.symbol = 'SOL'
        {% if is_incremental() %}
            AND p.minute >= date_trunc('day', now() - INTERVAL '1 week')
        {% endif %}
WHERE (
    array_contains(account_keys, '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y')
    OR array_contains(account_keys, 'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r')
)
{% if not is_incremental() %}
AND block_date > '2022-04-06'
AND block_slot > 128251864
{% endif %}
{% if is_incremental() %}
    AND block_date >= date_trunc('day', now() - INTERVAL '1 week')
{% endif %}
