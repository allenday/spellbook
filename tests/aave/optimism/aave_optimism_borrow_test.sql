WITH unit_test1
AS (SELECT COALESCE(usd_amount = 0.999989, FALSE) AS test
    FROM {{ ref('aave_optimism_borrow' ) }}
    WHERE token_address = '0x7f5c764cbc14f9669b88837ca1490cca17c31607'
          AND borrower = '0xf2eebb119be9313bafc22a031a29a94afc79e3c9'
          AND evt_block_time = '2022-10-11 16:05'
          AND evt_tx_hash = '0x27b52c00cf1b28b955b48c72b406b3f41f6abe6d4dfa1ea16b682fca414cb3da'
),

unit_test2
AS (SELECT COALESCE(amount = -1, FALSE) AS test
    FROM {{ ref('aave_optimism_borrow' ) }}
    WHERE symbol = 'DAI'
          AND repayer = '0xa9f12ca1b27941c05e3998279c06b81b8e33ca81'
          AND evt_block_time = '2022-10-11 10:11'
)

SELECT *
FROM (SELECT *
             FROM unit_test1
             UNION
             SELECT *
             FROM unit_test2)
WHERE test = FALSE
