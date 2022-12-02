{{ config(
        alias='erc20_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke", "dot2dotseurat"]\') }}'
        )
}}

WITH
days AS (
    SELECT explode(
        sequence(
            to_date('2015-01-01'), date_trunc('day', now()), INTERVAL 1 DAY
        )
    ) AS day
)

, daily_balances AS (SELECT
    wallet_address
    , token_address
    , amount_raw
    , amount
    , day
    , symbol
    , lead(
        day, 1, now()
    ) OVER (PARTITION BY token_address, wallet_address ORDER BY day) AS next_day
    FROM {{ ref('transfers_ethereum_erc20_rolling_day') }}
)

SELECT
    'ethereum' AS blockchain
    , days.day
    , daily_balances.wallet_address
    , daily_balances.token_address
    , daily_balances.amount_raw
    , daily_balances.amount
    , daily_balances.symbol
    , daily_balances.amount * p.price AS amount_usd
FROM daily_balances
INNER JOIN
    days ON
        daily_balances.day <= days.day AND days.day < daily_balances.next_day
LEFT JOIN {{ source('prices', 'usd') }} AS p
    ON p.contract_address = daily_balances.token_address
        AND days.day = p.minute
        AND p.blockchain = 'ethereum'
-- Removes rebase tokens FROM balances
LEFT JOIN {{ ref('tokens_ethereum_rebase') }} AS r
    ON daily_balances.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }} AS nc
    ON daily_balances.token_address = nc.token_address
WHERE r.contract_address IS NULL
    AND nc.token_address IS NULL
