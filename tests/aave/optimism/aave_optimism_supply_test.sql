WITH unit_test1
AS (SELECT COALESCE(usd_amount = 1.31305, FALSE) AS test
    FROM {{ ref('aave_optimism_supply' ) }}
    WHERE token_address = '0x4200000000000000000000000000000000000006'
          AND depositor = '0x76d3030728e52deb8848d5613abade88441cbc59'
          AND evt_block_time = '2022-10-10 10:27'
          AND evt_tx_hash = '0x792ac57b533bc85114f70792b78891b4e1a8daf206c2e0c75a426457657777b4'
),

unit_test2
AS (SELECT COALESCE(amount = -20, FALSE) AS test
    FROM {{ ref('aave_optimism_supply' ) }}
    WHERE symbol = 'USDC'
          AND depositor = '0x4ecb5300d9ec6bca09d66bfd8dcb532e3192dda1'
          AND evt_block_time = '2022-10-10 10:10'
)

SELECT *
FROM (SELECT *
             FROM unit_test1
             UNION
             SELECT *
             FROM unit_test2)
WHERE test = FALSE
