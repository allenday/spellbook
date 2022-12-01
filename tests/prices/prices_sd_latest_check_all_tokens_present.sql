-- Checking that all tokens from prices.usd_latest are in prices.usd

WITH unit_tests AS (
    SELECT COALESCE(pu.minute IS NOT NULL, FALSE) AS presence_test
    FROM {{ ref('prices_usd_latest') }} AS latest
    LEFT JOIN
        {{ source('prices', 'usd') }} AS pu ON pu.blockchain = latest.blockchain
            AND pu.contract_address = latest.contract_address
)

SELECT
    COUNT(
        CASE WHEN presence_test = FALSE THEN 1 END
    ) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN presence_test = FALSE THEN 1 END) > COUNT(*) * 0.05
