{{
  config(
        alias='daily_balances',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address','day'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH
updated_balances AS (
    SELECT
        address,
        date_trunc('day', block_time) AS day,
        token_mint_address,
        cast(post_balance AS double) / 1e9 AS sol_balance, --lamport -> sol 
        post_token_balance AS token_balance, --tokens are already correct decimals in this table
        token_balance_owner,
        row_number() OVER (PARTITION BY address, date_trunc('day', block_time) ORDER BY block_slot DESC) AS latest_balance
    FROM {{ source('solana','account_activity') }}
    WHERE
        tx_success
        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - interval '1 day')
        {% endif %}
)

SELECT
    day,
    address,
    sol_balance,
    token_mint_address,
    token_balance,
    token_balance_owner,
    now() AS updated_at
FROM updated_balances
WHERE latest_balance = 1
