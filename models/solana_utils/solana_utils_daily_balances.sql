 {{
  config(
        alias='daily_balances',
        materialized = 'view',
                incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address','day'])
}}

WITH 
      updated_balances as (
            SELECT
                  address 
                  , TIMESTAMP_TRUNC(block_time, day) AS `day`
                  , token_mint_address
                  , cast(post_balance as FLOAT64)/1e9 as sol_balance --lamport -> sol 
                  , post_token_balance as token_balance --tokens are already correct decimals in this table
                  , token_balance_owner
                  , row_number() OVER (partition by address, TIMESTAMP_TRUNC(block_time, day) order by block_slot desc) as latest_balance
            FROM {{ source('solana','account_activity') }}
            WHERE tx_success 
            {% if is_incremental() %}
            AND block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 day')
            {% endif %}
      )

SELECT 
      day
      , address
      , sol_balance
      , token_mint_address
      , token_balance
      , token_balance_owner
      , CURRENT_TIMESTAMP() as updated_at 
FROM updated_balances
WHERE latest_balance = 1