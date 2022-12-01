-- Bootstrapped correctness test against legacy Caladan values.

WITH sampled_wallets AS (
    SELECT *
    FROM {{ ref('balances_ethereum_erc20_day') }} AS bal
    WHERE
        wallet_address IN (
            SELECT DISTINCT wallet_address
            FROM {{ ref('balances_ethereum_erc20_daily_entries') }}
        )
        AND bal.day > '2021-12-30' AND bal.day < '2022-01-01'
),

unit_tests AS (
    SELECT COALESCE(
            ROUND(
                test_data.amount_raw / POWER(10, 18), 4
            ) = ROUND(sampled_wallets.amount_raw / POWER(10, 18), 4),
            FALSE) AS amount_raw_test
    FROM {{ ref('balances_ethereum_erc20_daily_entries') }} AS test_data
    INNER JOIN sampled_wallets
        ON test_data.timestamp = sampled_wallets.day
            AND test_data.wallet_address = sampled_wallets.wallet_address
            AND test_data.token_address = sampled_wallets.token_address
)

SELECT
    COUNT(
        CASE WHEN amount_raw_test = FALSE THEN 1 END
    ) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
-- Having mismatches less than 1% of rows
HAVING
    COUNT(CASE WHEN amount_raw_test = FALSE THEN 1 END) > COUNT(*) * 0.01
