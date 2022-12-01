WITH unit_test AS (
    SELECT
        COALESCE(ABS(test.oracle_price - actual.oracle_price) < 0.00001,
            FALSE) AS oracle_price_test,
        COALESCE(test.proxy_address = actual.proxy_address,
            FALSE) AS proxy_address_test,
        COALESCE(test.aggregator_address = actual.aggregator_address,
            FALSE) AS aggregator_address_test,
        COALESCE(
            test.underlying_token_address = actual.underlying_token_address,
            FALSE) AS underlying_token_address_test
    FROM {{ ref ('chainlink_polygon_price_feeds') }} AS actual
    INNER JOIN {{ ref ('chainlink_polygon_get_price') }} AS test
        ON (actual.blockchain = test.blockchain
            AND actual.block_number = test.block_number
            AND actual.feed_name = test.feed_name
            )
)

SELECT * FROM unit_test
WHERE (
    oracle_price_test = FALSE
    OR proxy_address_test = FALSE
    OR aggregator_address_test = FALSE
    OR underlying_token_address_test = FALSE
)
