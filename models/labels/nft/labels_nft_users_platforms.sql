{{config(alias='nft_users_platforms')}}

WITH nft_trades AS (
    SELECT
        blockchain
        , project
        , buyer AS address
    FROM {{ ref('nft_trades') }}
    UNION
    SELECT
        blockchain
        , project
        , seller AS address
    FROM {{ ref('nft_trades') }}
)

SELECT
    address
    , 'nft' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , collect_set(blockchain) AS blockchain
    , array_join(collect_set(concat(upper(SUBSTRING(project, 1, 1)), SUBSTRING(project, 2))), ', ') || ' User' AS name
    , timestamp('2022-09-03') AS created_at
    , now() AS updated_at
FROM nft_trades
WHERE address IS NOT NULL
GROUP BY address
