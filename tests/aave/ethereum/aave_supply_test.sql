WITH unit_test1
AS (SELECT COALESCE(usd_amount = -21150, FALSE) AS test
    FROM {{ ref('aave_ethereum_supply' ) }}
    WHERE token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
          AND depositor = '0x4767192455266e422386d14991d697a418c63225'
          AND evt_block_time = '2022-09-21 18:09'
          AND evt_tx_hash = '0x5be7cab6a33b1f1a050858e18a0a3140092440fbeb9e431c88103762f23d5305'
),

unit_test2
AS (SELECT COALESCE(amount = 250, FALSE) AS test
    FROM {{ ref('aave_ethereum_supply' ) }}
    WHERE symbol = 'SNX'
          AND depositor = '0x1e18b8b6f27568023e0b577cbee1889391b2f444'
          AND evt_block_time = '2022-09-24 00:05'
)

SELECT *
FROM (SELECT *
             FROM unit_test1
             UNION
             SELECT *
             FROM unit_test2)
WHERE test = FALSE
