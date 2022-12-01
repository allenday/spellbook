-- Manually checking ERC1155 balances on Zerion/DappRadar/Zaper on June 17th 2022
-- https://dappradar.com/hub/wallet/eth/0x09a5943a6d10919571ee2c9f63380aea747eca97/nfts-financial
-- Check Dune query on v2 here: https://dune.com/queries/919512?d=1 
WITH sampled_wallets AS (
    SELECT *
    FROM {{ ref('balances_ethereum_erc1155_june17th') }}
),

unit_tests AS (
    SELECT COALESCE(
        bal_day.amount = sampled_wallets.amount, FALSE
    ) AS amount_test
    FROM {{ ref('balances_ethereum_erc1155_day') }} AS bal_day
    INNER JOIN
        sampled_wallets ON
            sampled_wallets.wallet_address = bal_day.wallet_address
            AND sampled_wallets.token_address = bal_day.token_address
            AND sampled_wallets.tokenid = bal_day.tokenid
    WHERE
        bal_day.wallet_address = LOWER(
            '0x09a5943a6d10919571eE2C9F63380aEA747ECA97'
        )
        AND day = '2022-06-17'
)

SELECT
    COUNT(CASE WHEN amount_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
-- Having mismatches less than 1% of rows
HAVING
    COUNT(CASE WHEN amount_test = FALSE THEN 1 END) > COUNT(*) * 0.01
