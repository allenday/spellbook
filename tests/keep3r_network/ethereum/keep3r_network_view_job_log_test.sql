WITH unit_test AS (
    SELECT
        COALESCE(test.amount = ROUND(
            actual.amount,
            2
        ), FALSE) AS amount_test,
        COALESCE(LOWER(
            test.keeper
        ) = LOWER(
            actual.keeper
        ), FALSE) AS keeper_test,
        COALESCE(LOWER(
            test.token
        ) = LOWER(
            actual.token
        ), FALSE) AS token_test
    FROM
        {{ ref('keep3r_network_ethereum_view_job_log') }} AS actual
    INNER JOIN
        {{ ref('keep3r_network_ethereum_view_job_log_test_data') }} AS test
        ON LOWER(
            actual.tx_hash
        ) = LOWER(
            test.tx_hash
        )
)

SELECT *
FROM
    unit_test
WHERE
    amount_test = FALSE
    OR keeper_test = FALSE
    OR token_test = FALSE
