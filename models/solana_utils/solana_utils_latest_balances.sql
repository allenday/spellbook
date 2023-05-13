{{
  config(
        alias='latest_balances',
        materialized='table',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH
updated_balances AS (
    SELECT
        address,
        day,
        sol_balance,
        token_mint_address,
        token_balance,
        token_balance_owner,
        row_number() OVER (PARTITION BY address ORDER BY day DESC) AS latest_balance
    FROM {{ ref('solana_utils_daily_balances') }}
)

SELECT
    address,
    sol_balance,
    token_balance,
    token_mint_address,
    token_balance_owner,
    now() AS updated_at
FROM updated_balances
WHERE latest_balance = 1
