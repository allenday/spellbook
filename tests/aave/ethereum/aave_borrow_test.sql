WITH unit_test1
AS (SELECT COALESCE(usd_amount = 499785, FALSE) AS test
    FROM {{ ref('aave_ethereum_borrow' ) }}
    WHERE token_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
          AND borrower = '0xec46dd165ee2d4af460a9c3d01b5a4c9516c9c3f'
          AND evt_block_time = '2022-09-22 10:45'
          AND evt_tx_hash = '0x4c052a2865b828ef00dd2840870c08d2074930571c00c1f57f579acbab3e25c8'
),

unit_test2
AS (SELECT COALESCE(amount = -100, FALSE) AS test
    FROM {{ ref('aave_ethereum_borrow' ) }}
    WHERE symbol = 'DAI'
          AND repayer = '0x0d67d6aab7dab14da3b72ca70e0b83b74ad7e88f'
          AND evt_block_time = '2022-09-22 19:40'
)

SELECT *
FROM (SELECT *
             FROM unit_test1
             UNION
             SELECT *
             FROM unit_test2)
WHERE test = FALSE
