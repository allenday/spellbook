
{{config(alias='nft_traders_transactions')}}

WITH nft_trades AS (
    SELECT
        blockchain
        , tx_hash
        , buyer AS address
    FROM {{ ref('nft_trades') }}
    UNION
    SELECT
        blockchain
        , tx_hash
        , seller AS address
    FROM {{ ref('nft_trades') }}
)

, total AS (
    SELECT
        address
        , COUNT(tx_hash) AS total_count
    FROM nft_trades
    GROUP BY 1
)

SELECT
    collect_set(nft_trades.blockchain) AS blockchain
    , nft_trades.address
    , CASE WHEN ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 10
                                                 AND ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) > 5
            THEN 'Top 10% NFT Trader (Transaction)'
                                                 WHEN ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 5
            AND ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) > 1
            THEN 'Top 5% NFT Trader (Transactions)'
                                                 WHEN ((ROW_NUMBER() OVER(ORDER BY COUNT(tx_hash) DESC)) / total_count * 100) <= 1
            THEN 'Top 1% NFT Trader (Transactions)' END AS name
    , 'nft' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , timestamp('2022-08-24') AS created_at
    , now() AS updated_at
FROM nft_trades
JOIN total ON total.address = nft_trades.address
WHERE nft_trades.address is NOT NULL
GROUP BY nft_trades.address, total_count
