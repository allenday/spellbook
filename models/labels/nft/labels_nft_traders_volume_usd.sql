{{config(alias='nft_traders_volume_usd')}}

WITH nft_trades AS (
    SELECT
        blockchain
        , amount_usd
        , buyer AS address
    FROM {{ ref('nft_trades') }}
    UNION
    SELECT
        blockchain
        , amount_usd
        , seller AS address
    FROM {{ ref('nft_trades') }}
)

, total AS (
    SELECT
        address
        , SUM(amount_usd) AS total_count
    FROM nft_trades
    GROUP BY 1
)

SELECT
    nft_trades.address
    , 'nft' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , collect_set(nft_trades.blockchain) AS blockchain
    , CASE WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 10
                                                 AND ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) > 5
            THEN 'Top 10% NFT Trader (Volume in $USD)'
                                                 WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 5
            AND ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) > 1
            THEN 'Top 5% NFT Trader (Volume in $USD)'
                                                 WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 1
            THEN 'Top 1% NFT Trader (Volume in $USD)' END AS name
    , timestamp('2022-08-24') AS created_at
    , now() AS updated_at
FROM nft_trades
INNER JOIN total ON total.address = nft_trades.address
WHERE nft_trades.address IS NOT NULL AND amount_usd IS NOT NULL
GROUP BY nft_trades.address, total_count
