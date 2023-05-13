{{ config(
       alias = 'top_erc721_holders',
       materialized='table',
       post_hook='{{ expose_spells(\'["ethereum"]\',
                                   "sector",
                                   "nft",
                                   \'["Henrystats"]\') }}'
       )
   }}

WITH

erc721_balances AS (
    SELECT
        wallet_address,
        token_address AS nft_contract_address,
        COUNT(tokenid) AS balance
    FROM
        {{ ref('balances_ethereum_erc721_latest') }}
    GROUP BY 1, 2
),

total_supply AS (
    SELECT
        wallet_address,
        nft_contract_address,
        balance,
        SUM(balance) OVER (PARTITION BY nft_contract_address) AS total_supply
    FROM
        erc721_balances
)

SELECT *
FROM
    (
        SELECT
            wallet_address,
            nft_contract_address,
            balance,
            balance / total_supply AS supply_share,
            total_supply,
            ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY balance DESC) AS rn
        FROM
            total_supply
    ) AS x
WHERE rn <= 50
