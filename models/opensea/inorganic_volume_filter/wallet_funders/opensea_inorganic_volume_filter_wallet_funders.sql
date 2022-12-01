{{ config(
    alias = 'inorganic_volume_filter_wallet_funders',
    materialized = 'view'
)
}}

WITH

aggregators AS (
        SELECT contract_address AS aggregator_address FROM {{ ref('nft_ethereum_aggregators') }}
),

cex AS (
        SELECT address AS cex_address FROM {{ ref('labels_cex_ethereum') }}
),

disperse AS (
        SELECT LOWER('0xD152f549545093347A162Dce210e7293f1452150') AS disperse_address

),

contracts AS (
        SELECT address AS contract_address FROM {{ ref('labels_contracts') }}
        WHERE blockchain = array('ethereum')
),

funders AS (
        SELECT
            b.to AS wallet,
            MIN(a.from) AS funder,
            MIN(a.block_time) AS block_time,
            MIN(a.block_number) AS block_number,
            MIN(a.hash) AS tx_hash
        FROM
        {{ source('ethereum', 'transactions') }} a
        JOIN
        (
        SELECT
            MIN(block_number) AS first_block,
            to
        FROM
        {{ source('ethereum', 'transactions') }} et
        INNER JOIN
        {{ ref('opensea_inorganic_volume_filter_wallet_funders_wallets') }} wl
        ON et.to = wl.wallet
        LEFT JOIN aggregators agg ON et.from = agg.aggregator_address
        LEFT JOIN cex c ON et.from = c.cex_address
        LEFT JOIN disperse d ON et.from = d.disperse_address
        LEFT JOIN contracts c ON et.from = c.contract_address
        WHERE agg.aggregator_address IS NULL
        AND c.cex_address IS NULL
        AND d.disperse_address IS NULL
        AND c.contract_address IS NULL
        GROUP BY 2
        ) AS b
        ON a.to = b.to
        AND a.block_number = b.first_block
        GROUP BY 1
)

SELECT * FROM funders

