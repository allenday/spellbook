{{ config(
        alias='erc20_hour',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke", "dot2dotseurat"]\') }}'
        )
}}

WITH
hours AS (
    SELECT explode(
        sequence(
            to_date('2015-01-01'), date_trunc('hour', now()), INTERVAL 1 HOUR
        )
    ) AS hour
)

, hourly_balances AS (SELECT
    wallet_address
    , token_address
    , amount_raw
    , amount
    , hour
    , symbol
    , lead(hour, 1, now()) OVER (PARTITION BY token_address, wallet_address ORDER BY hour) AS next_hour
    FROM {{ ref('transfers_ethereum_erc20_rolling_hour') }}
)

SELECT
    'ethereum' AS blockchain
    , hours.hour
    , hourly_balances.wallet_address
    , hourly_balances.token_address
    , hourly_balances.amount_raw
    , hourly_balances.amount
    , hourly_balances.symbol
    , hourly_balances.amount * p.price AS amount_usd
FROM hourly_balances
INNER JOIN hours ON hourly_balances.hour <= hours.hour AND hours.hour < hourly_balances.next_hour
LEFT JOIN {{ source('prices', 'usd') }} AS p
    ON p.contract_address = hourly_balances.token_address
        AND hours.hour = p.minute
        AND p.blockchain = 'ethereum'
-- Removes rebase tokens FROM balances
LEFT JOIN {{ ref('tokens_ethereum_rebase') }} AS r
    ON hourly_balances.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }} AS nc
    ON hourly_balances.token_address = nc.token_address
WHERE r.contract_address IS NULL
    AND nc.token_address IS NULL
