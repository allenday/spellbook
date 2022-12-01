WITH unit_tests AS (
    SELECT COALESCE(
            GET_HREF(
                'https://etherscan.io/', 'etherscan'
            ) = CONCAT(
                '<a href="',
                'https://etherscan.io/',
                '"target ="_blank">',
                'etherscan'
            ),
            FALSE) AS test
)

SELECT COUNT(CASE WHEN test = FALSE THEN 1 END) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN test = FALSE THEN 1 END) > 0
