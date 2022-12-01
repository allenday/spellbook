-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows

WITH unit_tests AS (
    SELECT COALESCE(test_data_v1.tokenid = transfers_v2.tokenid, FALSE) AS test
    FROM {{ ref('test_view') }} AS transfers_v2
    INNER JOIN {{ ref('test_seed') }} AS test_data_v1
        ON test_data_v1.evt_tx_hash = transfers_v2.evt_tx_hash
            AND test_data_v1.value = ABS(transfers_v2.amount)
)

SELECT
    COUNT(CASE WHEN test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING COUNT(CASE WHEN test = FALSE THEN 1 END) > COUNT(*) * 0.05
