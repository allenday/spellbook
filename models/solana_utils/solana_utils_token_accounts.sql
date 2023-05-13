{{
  config(
        alias='token_accounts',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['token_mint_address', 'address'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

WITH
distinct_accounts AS (
    SELECT DISTINCT
        token_mint_address,
        address
    FROM {{ source('solana','account_activity') }}
    WHERE
        token_mint_address IS NOT NULL
        {% if is_incremental() %}
            AND block_time >= date_trunc("day", now() - interval "1 week")
        {% endif %}
)

SELECT
    *,
    now() AS updated_at
FROM distinct_accounts
