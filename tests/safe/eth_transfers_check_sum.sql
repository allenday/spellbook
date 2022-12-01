-- Check that Eth transfers amount for safes on a specific date is correct

WITH test_data AS (
    SELECT round(sum(amount_raw) / 1e18, 0) AS total
    FROM {{ ref('safe_ethereum_eth_transfers') }}
    WHERE block_time BETWEEN '2022-11-01' AND '2022-11-03'
),

test_result AS (
    SELECT coalesce(total = -1981, FALSE) AS success
    FROM test_data
)

SELECT *
FROM test_result
WHERE success = FALSE
