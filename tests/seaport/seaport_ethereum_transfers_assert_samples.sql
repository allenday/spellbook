-- Bootstrapped correctness test against legacy Postgres values.

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT COALESCE(
        test_data.original_amount = seaport_transfers.amount_original
        AND test_data.tx_hash = seaport_transfers.tx_hash
        AND test_data.block_time = seaport_transfers.block_time,
        FALSE) AS amount_test
    FROM {{ ref('seaport_ethereum_transfers') }} AS seaport_transfers
    INNER JOIN {{ ref('seaport_ethereum_transfers_postgres') }} AS test_data
        ON test_data.tx_hash = seaport_transfers.tx_hash
            AND seaport_transfers.block_time > '2022-06-14' AND seaport_transfers.block_time < '2022-06-16'

)

SELECT
    COUNT(CASE WHEN amount_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN amount_test = FALSE THEN 1 END) > COUNT(*) * 0.05
