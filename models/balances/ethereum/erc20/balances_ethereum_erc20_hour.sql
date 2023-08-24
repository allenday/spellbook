{{ config(
        alias='erc20_hour'
        )
}}

WITH hours AS (
  SELECT
    TIMESTAMP_ADD(
      TIMESTAMP '2015-01-01 00:00:00 UTC', 
      INTERVAL x HOUR
    ) AS `hour`
  FROM (
    SELECT row_number() OVER () - 1 AS x
    FROM (
      SELECT NULL
      FROM UNNEST(GENERATE_ARRAY(1, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP '2015-01-01 00:00:00 UTC', HOUR), 1)) -- generate enough numbers
    )
  )
)

, hourly_balances as
 (SELECT
    wallet_address,
    token_address,
    amount_raw,
    amount,
    `hour`,
    symbol,
    lead(hour, 1, CURRENT_TIMESTAMP()) OVER (PARTITION BY token_address, wallet_address ORDER BY `hour`) AS next_hour
    FROM {{ ref('transfers_ethereum_erc20_rolling_hour') }})

SELECT
    'ethereum' as blockchain,
    h.hour,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
FROM hourly_balances b
INNER JOIN hours h ON b.hour <= h.hour AND h.hour < b.next_hour
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND h.hour = p.minute
    AND p.blockchain = 'ethereum'
-- Removes rebase tokens from balances
LEFT JOIN {{ ref('tokens_ethereum_rebase') }}  as r
    ON b.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }}  as nc
    ON b.token_address = nc.token_address
WHERE r.contract_address is null
and nc.token_address is null