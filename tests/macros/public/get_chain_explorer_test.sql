WITH unit_tests AS (
    SELECT COALESCE(GET_CHAIN_EXPLORER('ethereum') = 'https://etherscan.io',
        FALSE) AS test
)

SELECT COUNT(CASE WHEN test = FALSE THEN 1 END) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN test = FALSE THEN 1 END) > 0
