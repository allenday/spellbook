-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT COALESCE(
            test_data.original_amount = lr_trades.amount_original, FALSE
        ) AS price_test
    FROM {{ ref('looksrare_ethereum_trades') }} AS lr_trades
    INNER JOIN
        {{ ref('looksrare_ethereum_trades_postgres') }} AS test_data ON
            test_data.tx_hash = lr_trades.tx_hash
)

SELECT
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) > COUNT(*) * 0.05
