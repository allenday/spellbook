 {{
  config(
        alias='token_accounts',
        materialized = 'view',
                incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address'])
}}

WITH 
      distinct_accounts as (
            SELECT
                  distinct 
                  token_mint_address
                  , address 
            FROM {{ source('solana','account_activity') }}
            WHERE token_mint_address is not null
            {% if is_incremental() %}
            AND block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
            {% endif %}
      )
      
SELECT *, CURRENT_TIMESTAMP() as updated_at FROM distinct_accounts