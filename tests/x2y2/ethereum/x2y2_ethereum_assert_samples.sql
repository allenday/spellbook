-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT COALESCE(
            test_data.original_amount = x2y2_trades.amount_original, FALSE
        ) AS price_test
    FROM {{ ref('nft_trades') }} AS x2y2_trades
    INNER JOIN
        {{ ref('x2y2_ethereum_trades_etherscan') }} AS test_data ON
            test_data.tx_hash = x2y2_trades.tx_hash
    WHERE x2y2_trades.project = 'x2y2'
        AND x2y2_trades.block_time = '2022-07-20' OR x2y2_trades.block_time = '2022-06-09'
)

SELECT
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) > COUNT(*) * 0.05
