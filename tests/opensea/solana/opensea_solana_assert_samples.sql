-- Bootstrapped correctness test against values downloaded from the Dune App 
-- The first 10 values were also manually checked using Solscan API

WITH unit_tests AS (
    SELECT COALESCE(test_data.amount = os_trades.amount_original,
        FALSE) AS price_test
    FROM {{ ref('opensea_solana_trades') }} AS os_trades
    INNER JOIN
        {{ ref('opensea_solana_trades_solscan') }} AS test_data ON
            test_data.tx_hash = os_trades.tx_hash
            AND test_data.block_time = os_trades.block_time
    WHERE
        os_trades.block_time > '2022-05-01' AND os_trades.block_time < '2022-05-03'
        AND os_trades.project = 'opensea' AND os_trades.blockchain = 'solana'
)

SELECT
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING COUNT(CASE WHEN price_test = FALSE THEN 1 END) > COUNT(*) * 0.1
