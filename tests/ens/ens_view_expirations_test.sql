-- Bootstrapped correctness test against legacy Postgres values.

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT COALESCE(test_data.label = ens_ve.label
        AND test_data.min_expires = ens_ve.min_expires,
        FALSE) AS min_expiration_test
    FROM {{ ref('ens_view_expirations') }} AS ens_ve
    INNER JOIN {{ ref('ens_view_expirations_postgres') }} AS test_data
        ON test_data.label = ens_ve.label
)

SELECT
    COUNT(*) AS count_rows,
    COUNT(
        CASE WHEN min_expiration_test = FALSE THEN 1 END
    ) / COUNT(*) AS pct_mismatch
FROM unit_tests
HAVING
    COUNT(CASE WHEN min_expiration_test = FALSE THEN 1 END) > COUNT(*) * 0.05
