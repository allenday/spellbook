{{ config(
        alias='erc20_hour',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke", "dot2dotseurat"]\') }}'
        )
}}

with
hours AS (
    SELECT
        explode(
            sequence(
                to_date('2015-01-01'), date_trunc('hour', now()), INTERVAL 1 hour
            )
        ) AS hour
)

, hourly_balances AS
(SELECT
    wallet_address,
    token_address,
    amount_raw,
    amount,
    hour,
    symbol,
    lead(hour, 1, now()) OVER (PARTITION BY token_address, wallet_address ORDER BY hour) AS next_hour
    FROM {{ ref('transfers_ethereum_erc20_rolling_hour') }})

SELECT
    'ethereum' AS blockchain,
    h.hour,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price AS amount_usd,
    b.symbol
FROM hourly_balances AS b
INNER JOIN hours AS h ON b.hour <= h.hour AND h.hour < b.next_hour
LEFT JOIN {{ source('prices', 'usd') }} AS p
    ON p.contract_address = b.token_address
        AND h.hour = p.minute
        AND p.blockchain = 'ethereum'
-- Removes rebase tokens FROM balances
LEFT JOIN {{ ref('tokens_ethereum_rebase') }}  AS r
    ON b.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }}  AS nc
    ON b.token_address = nc.token_address
WHERE r.contract_address is NULL
    AND nc.token_address is NULL
