WITH unit_test1
AS (
    SELECT COALESCE(ROUND(variable_borrow_apy, 17) = 0.02410603665276986,
        FALSE) AS test
    FROM {{ ref('aave_v2_ethereum_interest_rates' ) }}
    WHERE reserve = '0xdac17f958d2ee523a2206206994597c13d831ec7'
          AND hour = '2022-08-22 12:00'
),

unit_test2
AS (SELECT COALESCE(ROUND(deposit_apy, 17) = 0.00422367473269522, FALSE) AS test
    FROM {{ ref('aave_v2_ethereum_interest_rates' ) }}
    WHERE symbol = 'USDC'
          AND hour = '2022-08-25 09:00'
)

SELECT *
FROM (SELECT *
             FROM unit_test1
             UNION
             SELECT *
             FROM unit_test2)
WHERE test = FALSE
