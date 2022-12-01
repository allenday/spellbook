-- Check against manually selected seed data
WITH unit_tests AS (
    SELECT COALESCE(
            test_data.original_amount_raw = nomad_transactions.original_amount_raw,
            FALSE
        ) AS amount_test
    FROM
        {{ ref('nomad_ethereum_view_bridge_transactions') }} AS nomad_transactions
    INNER JOIN
        {{ ref('nomad_ethereum_transactions_etherscan') }} AS test_data ON
            test_data.tx_hash = nomad_transactions.tx_hash
    WHERE
        nomad_transactions.block_time >= '2022-07-27' AND nomad_transactions.block_time < '2022-07-28'
)

SELECT * FROM unit_tests WHERE amount_test = FALSE
