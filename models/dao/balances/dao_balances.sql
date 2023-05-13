{{ config(
    alias = 'balances',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}

{% set project_start_date = '2018-10-27' %}

WITH balances AS (
    SELECT
        block_date AS day,
        SUM(value) AS value,
        dao,
        dao_wallet_address,
        dao_creator_tool,
        blockchain,
        asset_contract_address,
        asset
    FROM
        {{ ref('dao_transactions') }}
    WHERE tx_type = 'tx_in'
    GROUP BY 1, 3, 4, 5, 6, 7, 8

    UNION ALL

    SELECT
        block_date AS day,
        -1 * SUM(value) AS value,
        dao,
        dao_wallet_address,
        dao_creator_tool,
        blockchain,
        asset_contract_address,
        asset
    FROM
        {{ ref('dao_transactions') }}
    WHERE tx_type = 'tx_out'
    GROUP BY 1, 3, 4, 5, 6, 7, 8
),

balances_all AS (
    SELECT
        day,
        SUM(value) AS value,
        dao,
        dao_wallet_address,
        dao_creator_tool,
        blockchain,
        asset_contract_address,
        asset
    FROM balances
    GROUP BY 1, 3, 4, 5, 6, 7, 8
),

days AS (
    SELECT explode(
        sequence(
            to_date('{{ project_start_date }}'), date_trunc('day', now()), INTERVAL 1 DAY
        )
    ) AS day
),

daily_balances AS (
    SELECT
        *,
        SUM(value)
            OVER (PARTITION BY dao, dao_wallet_address, asset_contract_address, blockchain ORDER BY day) AS balance,
        lead(DAY, 1, now())
            OVER (PARTITION BY dao, dao_wallet_address, asset_contract_address, blockchain ORDER BY day) AS next_day
    FROM balances_all
)

SELECT
    d.day,
    db.blockchain,
    db.dao_creator_tool,
    db.dao,
    db.dao_wallet_address,
    db.balance,
    db.balance * p.price AS usd_value,
    db.asset,
    db.asset_contract_address
FROM daily_balances AS db
INNER JOIN days AS d
    ON
        db.day <= d.day
        AND d.day < db.next_day
LEFT JOIN
    {{ source('prices', 'usd') }} AS p
    ON
        p.contract_address = db.asset_contract_address
        AND d.day = p.minute
        AND p.blockchain = db.blockchain
