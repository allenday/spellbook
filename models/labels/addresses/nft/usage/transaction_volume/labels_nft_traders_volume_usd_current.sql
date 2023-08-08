{{config(alias='nft_traders_volume_usd_current')}}

WITH nft_trades AS (
SELECT
    blockchain,
    amount_usd,
    buyer AS address
FROM {{ ref('nft_trades') }}
WHERE block_time > CURRENT_TIMESTAMP() - interval '14' day

UNION ALL

SELECT
    blockchain,
    amount_usd,
    seller AS address
FROM {{ ref('nft_trades') }}
WHERE block_time > CURRENT_TIMESTAMP() - interval '14' day
),

total as (
SELECT
address,
SUM(amount_usd) AS total_count
FROM nft_trades
GROUP BY 1
)

SELECT * FROM (
    SELECT
    nft_trades.blockchain as blockchain,
    nft_trades.address,
    CASE WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 10
              AND ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) > 5
            THEN 'Current Top MOD( 10, NFT) Trader (Volume in $USD)'
         WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 5
              AND ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) > 1
            THEN 'Current Top MOD( 5, NFT) Trader (Volume in $USD)'
         WHEN ((ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC)) / total_count * 100) <= 1
            THEN 'Current Top MOD( 1, NFT) Trader (Volume in $USD)' END AS name,
    'nft' AS category,
    'hildobby' AS contributor,
    'query' AS source,
    timestamp('2023-03-29') as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    'nft_traders_volume_usd_current' as model_name,
    'usage' as label_type
    FROM nft_trades
      JOIN total on total.address = nft_trades.address
    WHERE nft_trades.address is not null and amount_usd is not null
    GROUP BY nft_trades.address, total_count, nft_trades.blockchain
)
WHERE name is not null