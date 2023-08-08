{{ config(
        alias ='nft'
        , materialized = 'view'
        
        )
}}

-- -- reservoir has multiple collection names for single contracts, we just take the first record
-- WITH reservoir_names as (
--     select
--     contract as contract_address
--     ,ARRAY_AGG(name ORDER BY created_at ASC LIMIT 1)[OFFSET(0)] as name
--     FROM {{ source('reservoir','collections') }} c
--     group by 1
-- )

SELECT
    c.contract_address
  , curated.name AS name
  , curated.symbol
  , c.standard
FROM {{ ref('tokens_ethereum_nft_standards') }} AS c
LEFT JOIN {{ ref('tokens_ethereum_nft_curated') }} AS curated
    ON c.contract_address = curated.contract_address
