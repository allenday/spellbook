-- Bootstrapped correctness test against legacy Postgres values.

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT COALESCE(
            test_data.original_amount = seaport_trades.original_amount AND test_data.tx_hash = seaport_trades.tx_hash,
            FALSE
        ) AS amount_test
    FROM {{ ref('seaport_ethereum_view_transactions') }} AS seaport_trades
    INNER JOIN
        {{ ref('seaport_ethereum_view_transactions_postgres') }} AS test_data ON
            test_data.tx_hash = seaport_trades.tx_hash
)

SELECT
    COUNT(CASE WHEN amount_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN amount_test = FALSE THEN 1 END) > COUNT(*) * 0.05
