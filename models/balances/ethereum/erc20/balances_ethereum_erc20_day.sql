{{ config(
        alias='erc20_day'
        )
}}

WITH days AS (
  SELECT
    TIMESTAMP_ADD(
      TIMESTAMP '2015-01-01 00:00:00 UTC', 
      INTERVAL x DAY
    ) AS `day`
  FROM (
    SELECT row_number() OVER () - 1 AS x
    FROM (
      SELECT NULL
      FROM UNNEST(GENERATE_ARRAY(1, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP '2015-01-01 00:00:00 UTC', DAY), 1)) -- generate enough numbers
    )
  )
)

, daily_balances as
 (SELECT
    wallet_address,
    token_address,
    amount_raw,
    amount,
    `day`,
    symbol,
    lead(day, 1, CURRENT_TIMESTAMP()) OVER (PARTITION BY token_address, wallet_address ORDER BY `day`) AS next_day
    FROM {{ ref('transfers_ethereum_erc20_rolling_day') }})

SELECT
    'ethereum' as blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
FROM daily_balances b
INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.day = p.minute
    AND p.blockchain = 'ethereum'
-- Removes rebase tokens from balances
LEFT JOIN {{ ref('tokens_ethereum_rebase') }}  as r
    ON b.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }}  as nc
    ON b.token_address = nc.token_address
WHERE r.contract_address is null
and nc.token_address is null