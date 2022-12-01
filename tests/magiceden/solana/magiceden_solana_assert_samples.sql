-- Bootstrapped correctness test against values downloaded from the Dune App 
-- The first 10 values were also manually checked using Solscan API

WITH unit_tests AS (
    SELECT COALESCE(test_data.amount = me_trades.amount_original,
        FALSE) AS price_test
    FROM {{ ref('nft_trades') }} AS me_trades
    INNER JOIN
        {{ ref('magiceden_solana_trades_solscan') }} AS test_data ON
            test_data.tx_hash = me_trades.tx_hash
            AND test_data.block_time = me_trades.block_time
    WHERE
        me_trades.block_time > '2021-10-23' AND me_trades.block_time < '2021-10-25'
        AND me_trades.project = 'magiceden' AND me_trades.blockchain = 'solana'
)

SELECT
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING COUNT(CASE WHEN price_test = FALSE THEN 1 END) > COUNT(*) * 0.1
