-- Check that safes count on a specific date is correct

WITH test_data AS (
    SELECT count(*) AS total
    FROM {{ ref('safe_ethereum_safes') }}
    WHERE creation_time < '2022-08-03'
),

test_result AS (
    SELECT coalesce(total = 84999, FALSE) AS success
    FROM test_data
)

SELECT *
FROM test_result
WHERE success = FALSE
