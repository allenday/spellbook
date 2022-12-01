WITH unit_test1
AS (SELECT COALESCE(variable_borrow_apy = 0.03485010188503055, FALSE) AS test
    FROM {{ ref('aave_v3_optimism_interest_rates' ) }}
    WHERE reserve = '0x7f5c764cbc14f9669b88837ca1490cca17c31607'
          AND hour = '2022-09-11 03:00'
),

unit_test2
AS (SELECT COALESCE(deposit_apy = 0.02788476319193648, FALSE) AS test
    FROM {{ ref('aave_v3_optimism_interest_rates' ) }}
    WHERE symbol = 'sUSD'
          AND hour = '2022-08-25 09:00'
)

SELECT *
FROM (SELECT *
             FROM unit_test1
             UNION
             SELECT *
             FROM unit_test2)
WHERE test = FALSE
