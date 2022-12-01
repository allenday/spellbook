-- Bootstrapped correctness test against legacy Caladan values.

WITH sampled_wallets AS (
    SELECT *
    FROM {{ ref('balances_ethereum_erc20_hour') }} AS bal
    WHERE
        wallet_address IN (
            SELECT DISTINCT wallet_address
            FROM {{ ref('balances_ethereum_erc20_latest_entries') }}
        )
        AND symbol IN ('USDT', 'LINK', 'DAI', 'USDC')
        AND bal.hour > '2022-05-04' AND bal.hour < '2022-05-06'
),

unit_tests AS (
    SELECT COALESCE(
            ROUND(
                test_data.amount_raw / POWER(10, 22), 3
            ) = ROUND(sampled_wallets.amount_raw / POWER(10, 22), 3),
            FALSE) AS amount_raw_test
    FROM {{ ref('balances_ethereum_erc20_latest_entries') }} AS test_data
    INNER JOIN sampled_wallets
        ON test_data.timestamp = sampled_wallets.hour
            AND test_data.wallet_address = sampled_wallets.wallet_address
            AND test_data.token_address = sampled_wallets.token_address
)

SELECT
    COUNT(
        CASE WHEN amount_raw_test = FALSE THEN 1 END
    ) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
-- Having mismatches less than 5% of rows
HAVING
    COUNT(CASE WHEN amount_raw_test = FALSE THEN 1 END) > COUNT(*) * 0.05
